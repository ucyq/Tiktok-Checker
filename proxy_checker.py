#!/usr/bin/env python3
"""
Fast proxy checker (asyncio + aiohttp).

Reads proxies.txt and writes:
  - alive.txt -> proxies that work
  - dead.txt  -> proxies that failed

Accepted formats per line:
  ip:port
  user:pass@ip:port
  http://ip:port | socks4://ip:port | socks5://user:pass@ip:port

Install:
  pip install aiohttp aiohttp-socks
  (optional, Linux/Mac only, faster: pip install uvloop)

Usage:
  python proxy_checker.py
  python proxy_checker.py -i list1.txt -c 1500 -t 6 --type socks5
"""

import argparse
import asyncio
import sys
import time

import aiohttp
from aiohttp_socks import ProxyConnector

VALID_SCHEMES = ("http", "https", "socks4", "socks5")


def parse_proxy(line: str, default_type: str):
    """Returns (original_line, proxy_url) or None if the line is invalid."""
    original = line.strip()
    if not original or original.startswith("#"):
        return None

    if "://" in original:
        scheme, rest = original.split("://", 1)
        scheme = scheme.lower()
    else:
        scheme, rest = default_type, original

    if scheme not in VALID_SCHEMES:
        return None

    hostport = rest.rsplit("@", 1)[-1]
    if ":" not in hostport:
        return None
    host, port = hostport.rsplit(":", 1)
    if not host or not port.isdigit() or not (0 < int(port) < 65536):
        return None

    return original, f"{scheme}://{rest}"


async def check_proxy(proxy_url: str, session: aiohttp.ClientSession, args) -> bool:
    timeout = aiohttp.ClientTimeout(total=args.timeout, sock_connect=args.timeout)
    try:
        if proxy_url.startswith(("socks4", "socks5")):
            connector = ProxyConnector.from_url(proxy_url, force_close=True)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
                async with s.get(args.url, allow_redirects=False) as r:
                    return r.status == args.status
        else:
            async with session.get(
                args.url, proxy=proxy_url, timeout=timeout, allow_redirects=False
            ) as r:
                return r.status == args.status
    except Exception:
        return False


async def worker(queue: asyncio.Queue, session, args, stats, f_alive, f_dead):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        original, proxy_url = item
        try:
            ok = await asyncio.wait_for(
                check_proxy(proxy_url, session, args), timeout=args.timeout + 2
            )
        except Exception:
            ok = False

        if ok:
            f_alive.write(original + "\n")
            stats["alive"] += 1
        else:
            f_dead.write(original + "\n")
            stats["dead"] += 1
        stats["done"] += 1
        queue.task_done()


async def progress(stats, total, f_alive, f_dead, start):
    while True:
        await asyncio.sleep(1)
        f_alive.flush()
        f_dead.flush()
        done = stats["done"]
        elapsed = max(time.time() - start, 0.001)
        rate = done / elapsed
        eta = (total - done) / rate if rate > 0 else 0
        sys.stdout.write(
            f"\r[{done}/{total}] alive: {stats['alive']} | dead: {stats['dead']} "
            f"| {rate:.0f}/s | ETA: {int(eta // 60)}m{int(eta % 60):02d}s   "
        )
        sys.stdout.flush()


def raise_fd_limit(wanted: int):
    """On Linux/Mac, raises the open file limit (each connection uses one)."""
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = wanted if hard == resource.RLIM_INFINITY else min(wanted, hard)
        if target > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    except Exception:
        pass


async def main(args):
    # Read and dedupe while preserving order
    proxies, seen, invalid = [], set(), 0
    with open(args.input, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = parse_proxy(line, args.type)
            if parsed is None:
                if line.strip() and not line.strip().startswith("#"):
                    invalid += 1
                continue
            if parsed[1] in seen:
                continue
            seen.add(parsed[1])
            proxies.append(parsed)
    del seen

    total = len(proxies)
    if total == 0:
        print("No valid proxies found in", args.input)
        return
    print(f"{total} unique proxies loaded ({invalid} invalid lines ignored).")
    print(f"Concurrency: {args.concurrency} | timeout: {args.timeout}s | test: {args.url}")

    raise_fd_limit(args.concurrency * 2 + 200)

    mode = "a" if args.append else "w"
    f_alive = open(args.alive, mode, encoding="utf-8", buffering=1 << 16)
    f_dead = open(args.dead, mode, encoding="utf-8", buffering=1 << 16)

    stats = {"done": 0, "alive": 0, "dead": 0}
    queue: asyncio.Queue = asyncio.Queue(maxsize=args.concurrency * 2)
    connector = aiohttp.TCPConnector(limit=0, force_close=True, ttl_dns_cache=300)
    start = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [
            asyncio.create_task(worker(queue, session, args, stats, f_alive, f_dead))
            for _ in range(args.concurrency)
        ]
        prog = asyncio.create_task(progress(stats, total, f_alive, f_dead, start))
        try:
            for item in proxies:
                await queue.put(item)
            for _ in workers:
                await queue.put(None)
            await asyncio.gather(*workers)
        finally:
            prog.cancel()
            for w in workers:
                w.cancel()
            f_alive.close()
            f_dead.close()

    elapsed = time.time() - start
    print(
        f"\nFinished in {elapsed:.1f}s -> alive: {stats['alive']} ({args.alive}) | "
        f"dead: {stats['dead']} ({args.dead})"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Fast proxy checker")
    p.add_argument("-i", "--input", default="proxies.txt", help="input file")
    p.add_argument("--alive", default="alive.txt", help="alive proxies file")
    p.add_argument("--dead", default="dead.txt", help="dead proxies file")
    p.add_argument("-c", "--concurrency", type=int, default=500, help="simultaneous checks")
    p.add_argument("-t", "--timeout", type=float, default=8, help="timeout in seconds")
    p.add_argument(
        "--type",
        default="http",
        choices=VALID_SCHEMES,
        help="default type for lines without a scheme (ip:port)",
    )
    p.add_argument(
        "--url",
        default="https://www.gstatic.com/generate_204",
        help="URL used for testing (https = tests the CONNECT tunnel)",
    )
    p.add_argument("--status", type=int, default=204, help="expected HTTP status")
    p.add_argument("--append", action="store_true", help="append to files instead of overwriting")
    return p.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    try:
        import uvloop  # type: ignore

        uvloop.install()
    except ImportError:
        pass
    try:
        asyncio.run(main(arguments))
    except KeyboardInterrupt:
        print("\nInterrupted. Partial results were saved.")
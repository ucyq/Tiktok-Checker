import random, threading, time, os, sys, heapq
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.cookiejar import DefaultCookiePolicy

try:
    import requests, uuid, colorama, cursor, json
    import bs4
    from colorama import Fore, Back, Style
except ImportError as e:
    input(f"Package {e} is not installed")
    sys.exit(f"Install {e}")

proxyDebug = False          # True = shows each individual proxy failure

# ---- Configuration ----
PREVALIDATE = True          # test all proxies via HTTPS (CONNECT) before starting
TEST_URL = "https://www.tiktok.com/robots.txt"
OK_STATUS = {200, 204, 301, 302, 304, 404, 429}   # responses that prove the proxy works
PREVALIDATE_THREADS = 300
PRE_TIMEOUT = 8             
CONNECT_TIMEOUT = 8         
READ_TIMEOUT = 15           
MAX_PROXY_FAILS = 3         
THREADS_PER_PROXY = 4       
MAX_THREADS = 500           
STATS_INTERVAL = 3          

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
]

import ipaddress

_print = print
_print_lock = threading.Lock()
def print(*args, **kwargs):
    with _print_lock:
        _print(*args, **kwargs)

SCHEMES = {"http": "http", "https": "http", "socks4": "socks4a", "socks4a": "socks4a",
           "socks5": "socks5h", "socks5h": "socks5h"}

def normalize_proxy(item):
    """Accepts ip:port, user:pass@ip:port or with scheme (http://, socks5://...).
    Returns the full proxy URL (with scheme) or None if invalid."""
    scheme = "http"
    if "://" in item:
        scheme, item = item.split("://", 1)
        scheme = scheme.lower()
    if scheme not in SCHEMES:
        return None
    hostport = item.split("@")[-1]
    host, _, port = hostport.rpartition(":")
    host = host.strip("[]")
    if not host or not port.isdigit() or not (1 <= int(port) <= 65535):
        return None
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_multicast or ip.is_reserved or ip.is_unspecified:
            return None
    except ValueError:
        pass
    return f"{SCHEMES[scheme]}://{item}"

def clean_proxies(lines):
    good, bad, seen = [], [], set()
    for line in lines:
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        url = normalize_proxy(item)
        if url is None:
            bad.append(item)
            continue
        if url in seen:
            continue
        seen.add(url)
        good.append(url)
    return good, bad

def error_kind(e):
    """Groups requests errors into short categories for the [stats] summary."""
    ex = requests.exceptions
    if isinstance(e, ex.ConnectTimeout):
        return "connect timeout"
    if isinstance(e, ex.ReadTimeout):
        return "read timeout"
    if isinstance(e, ex.ProxyError):
        return "proxy refused/error"
    if isinstance(e, ex.SSLError):
        return "ssl"
    if isinstance(e, ex.ChunkedEncodingError):
        return "response cut"
    if isinstance(e, ex.ConnectionError):
        return "connection closed"
    return type(e).__name__

def proxy_alive(proxy):
    """Tests whether the proxy can open an HTTPS tunnel (CONNECT) and receive a valid HTTP response."""
    try:
        r = requests.get(
            TEST_URL,
            proxies={"http": proxy, "https": proxy},
            headers={"user-agent": USER_AGENTS[0], "connection": "close"},
            timeout=(PRE_TIMEOUT, PRE_TIMEOUT),
            allow_redirects=False,
            stream=True,
        )
        status = r.status_code
        r.close()
        return status in OK_STATUS
    except Exception:
        return False

class Checker:
    def __init__(self):
        colorama.init(convert=True, autoreset=True)
        cursor.hide()
        self.mag = colorama.Fore.MAGENTA
        self.grn = colorama.Fore.GREEN
        self.white = colorama.Fore.WHITE
        self.blue = colorama.Fore.CYAN
        self.sample = "abcdefghijklmnopqrstuvwxyz0123456789_"
        self.num = 4
        self.threads = MAX_THREADS 
        self.hits = 0
        self.fails = 0
        self.errors = 0
        self.rates = 0
        self.proxies, self.bad_proxies = clean_proxies(open("proxy.txt", "r").read().splitlines())
        if not self.proxies:
            input("proxy.txt is empty or contains only invalid proxies")
            sys.exit("No valid proxies")
        self.per_proxy = THREADS_PER_PROXY
        self.heap = []
        self.dead = set()
        if any(p.startswith('socks') for p in self.proxies):
            try:
                import socks  # noqa: F401
            except ImportError:
                input('There are SOCKS proxies in proxy.txt: run  pip install requests[socks]  (PySocks)')
                sys.exit('PySocks not installed')
        self.cooldown = {}
        self.proxy_fails = {}
        self.limit_hits = {}
        self.last_warn = 0
        self.lock = threading.Lock()
        self.session = requests.Session()
        # Do not query environment variables/Windows registry/.netrc on every request (slow with many threads)
        self.session.trust_env = False
        self.stats_lock = threading.Lock()
        self.st_done, self.st_ttfb, self.st_total, self.st_err = 0, 0.0, 0.0, Counter()
        # No shared cookies: previously the same TikTok cookie was resent by different proxies/IPs
        self.session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))
        # No automatic retry: if the proxy failed, the right move is to switch proxies, not insist on the same one
        self.session.mount(
            "https://",
            HTTPAdapter(max_retries=0, pool_connections=self.threads, pool_maxsize=self.threads),
        )

        os.system("cls" if os.name == "nt" else "clear")
        print(
            f"""{self.mag}
                                                                    
                                                               
 $$$$$$$\ $$$$$$$\   $$$$$$\   $$$$$$$\ $$ |  $$\  $$$$$$\   $$$$$$\  
$$  _____|$$  __$$\ $$  __$$\ $$  _____|$$ | $$  |$$  __$$\ $$  __$$\ 
$$ /      $$ |  $$ |$$$$$$$$ |$$ /      $$$$$$  / $$$$$$$$ |$$ |  \__|
$$ |      $$ |  $$ |$$   ____|$$ |      $$  _$$<  $$   ____|$$ |      
\$$$$$$$\ $$ |  $$ |\$$$$$$$\ \$$$$$$$\ $$ | \$$\ \$$$$$$$\ $$ |      
 \_______|\__|  \__| \_______| \_______|\__|  \__| \_______|\__|      
    {self.white}
    By oqhy © 2026 ~ 0xfff0800\n""",
            "\n\n",
        )

        loaded = len(self.proxies)
        if PREVALIDATE:
            self.proxies = self.prevalidate(self.proxies)
            if not self.proxies:
                input("No proxy was able to open an HTTPS tunnel. Use HTTP/HTTPS proxies with CONNECT support or SOCKS.")
                sys.exit("No working proxies")

        self.heap = [(0, random.random(), p) for p in self.proxies for _ in range(self.per_proxy)]
        random.shuffle(self.heap)
        heapq.heapify(self.heap)

        workers = min(self.threads, len(self.proxies) * self.per_proxy)
        print(f"    {Fore.YELLOW}{len(self.proxies)}/{loaded} active proxies, {len(self.bad_proxies)} invalid ignored | {workers} threads ({self.per_proxy} per proxy){Fore.RESET}\n")

        for _ in range(workers):
            threading.Thread(target=self.check2, daemon=True).start()
        threading.Thread(target=self.report_stats, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"    [ {Fore.RED}$ {self.white}]{Fore.RED} Exit ...")
            os._exit(0)

    def pick_proxy(self):
        while True:
            now = time.time()
            with self.lock:
                if self.heap and self.heap[0][0] <= now:
                    return heapq.heappop(self.heap)[2]
                soonest = self.heap[0][0] - now if self.heap else None
            if soonest is not None:
                self.warn_all_waiting(soonest)
            time.sleep(0.05)

    def release_proxy(self, proxy):
        with self.lock:
            if proxy in self.dead:
                return
            heapq.heappush(self.heap, (self.cooldown.get(proxy, 0), random.random(), proxy))

    def proxy_ok(self, proxy):
        with self.lock:
            self.proxy_fails[proxy] = 0
            self.limit_hits[proxy] = 0

    def proxy_failed(self, proxy, reason=""):
        """Counts a failure. After MAX_PROXY_FAILS in a row, the proxy is removed from rotation permanently."""
        with self.lock:
            if proxy in self.dead:
                return
            n = self.proxy_fails.get(proxy, 0) + 1
            self.proxy_fails[proxy] = n
            if n < MAX_PROXY_FAILS:
                self.cooldown[proxy] = time.time() + 2 * n
                return
            self.dead.add(proxy)
            self.heap = [e for e in self.heap if e[2] != proxy]
            heapq.heapify(self.heap)
            alive = len(self.proxies) - len(self.dead)
            print(f"    {Fore.RED}Proxy removed ({n} consecutive failures): {proxy} -> {reason} | remaining: {alive}{Fore.RESET}")
            if alive <= 0:
                print(f"    {Fore.RED}All proxies were removed. Exiting.{Fore.RESET}")
                os._exit(1)

    def record_ok(self, started, ttfb):
        with self.stats_lock:
            self.st_done += 1
            self.st_ttfb += ttfb
            self.st_total += time.time() - started

    def record_err(self, kind):
        self.errors += 1
        with self.stats_lock:
            self.st_err[kind] += 1

    def report_stats(self):
        last = time.time()
        while True:
            time.sleep(STATS_INTERVAL)
            with self.stats_lock:
                done, ttfb, total, errs = self.st_done, self.st_ttfb, self.st_total, self.st_err
                self.st_done, self.st_ttfb, self.st_total, self.st_err = 0, 0.0, 0.0, Counter()
            now = time.time()
            dt = max(now - last, 0.001)
            last = now
            n_err = sum(errs.values())
            if done:
                lat = f"{total / done:.1f}s (connect+response {ttfb / done:.1f}s, download {(total - ttfb) / done:.1f}s)"
            else:
                lat = "-"
            top = ", ".join(f"{k} {v}" for k, v in errs.most_common(4)) or "none"
            alive = len(self.proxies) - len(self.dead)
            print(f"    {Fore.YELLOW}[stats] {done / dt:.1f} checks/s | avg latency {lat} | "
                  f"errors {n_err / dt:.1f}/s ({top}) | total errors {self.errors} | active proxies {alive}{Fore.RESET}")

    def prevalidate(self, proxies):
        total = len(proxies)
        print(f"    {Fore.YELLOW}Testing {total} proxies via HTTPS at {TEST_URL} (timeout {PRE_TIMEOUT}s)...{Fore.RESET}")
        good, done, t0 = [], 0, time.time()
        try:
            with ThreadPoolExecutor(max_workers=max(1, min(PREVALIDATE_THREADS, total))) as ex:
                for proxy, ok in zip(proxies, ex.map(proxy_alive, proxies)):
                    done += 1
                    if ok:
                        good.append(proxy)
                    if done % 50 == 0 or done == total:
                        print(f"    {Fore.YELLOW}[{done}/{total}] working: {len(good)}{Fore.RESET}", end="\r", flush=True)
        except KeyboardInterrupt:
            print(f"\n    [ {Fore.RED}$ {self.white}]{Fore.RED} Exit ...")
            os._exit(0)
        print(f"\n    {Fore.YELLOW}Test finished in {time.time() - t0:.0f}s: {len(good)} ok, {total - len(good)} discarded{Fore.RESET}\n")
        return good

    def proxy_limited(self, proxy):
        with self.lock:
            now = time.time()
            if self.cooldown.get(proxy, 0) > now:
                return
            n = self.limit_hits.get(proxy, 0) + 1
            self.limit_hits[proxy] = n
            self.cooldown[proxy] = now + min(60, 3 * 2 ** (n - 1))

    def warn_all_waiting(self, soonest):
        now = time.time()
        with self.lock:
            if now - self.last_warn >= 5:
                self.last_warn = now
                print(f"    {Fore.YELLOW}All free proxies are on cooldown, the first returns in ~{max(0, soonest):.0f}s{Fore.RESET}")

    def check2(self):
        while True:
            proxy = None
            response = None
            try:
                username = "".join(random.choices(self.sample, k=self.num))
                proxy = self.pick_proxy(); proxyDict = {"http": proxy, "https": proxy}
                if proxyDebug:
                    print(f"{Fore.MAGENTA}[{Fore.RESET}!{Fore.MAGENTA}] {Fore.RESET}Using proxy: {Fore.GREEN}{proxyDict}{Fore.RESET}")
                
                named_tuple = time.localtime() 
                self.time = time.strftime("%H:%M:%S", named_tuple)
                headers = {
                    "Host": "www.tiktok.com",
                    "user-agent": random.choice(USER_AGENTS),
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "accept-language": "en-US,en;q=0.9",
                    "accept-encoding": "gzip, deflate",
                }
                t0 = time.time()
                response = self.session.get(
                    f"https://www.tiktok.com/@{username}", headers=headers, proxies=proxyDict,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), stream=True
                )

                if response.status_code == 200:
                    body = response.content
                    self.proxy_ok(proxy)
                    self.record_ok(t0, response.elapsed.total_seconds())
                    if b'"statusCode":10221' in body:
                        print(
                        f"    [ {self.grn}$ {self.white}]{self.grn} Username Available {self.blue}{username}{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                        print(username, file=open("available.txt", "a"))
                        self.hits += 1
                    else:
                        self.fails += 1
                        print(
                        f"    [ {self.grn}$ {self.white}]{Fore.RED} Not Available {self.blue}{username}{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                elif response.status_code == 404:
                    self.proxy_ok(proxy)
                    self.record_ok(t0, response.elapsed.total_seconds())
                    print(
                        f"    [ {self.grn}$ {self.white}]{self.grn} Available/Banned {self.blue}{username}{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                    print(username, file=open("available.txt", "a"))
                    self.hits += 1

                elif response.status_code == 429:
                    self.fails += 1
                    self.rates += 1
                    self.record_err("429 (limit)")
                    print(
                        f"    [ {self.grn}$ {self.white}]{Fore.RED} Connection blocked {self.white}429{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                    self.proxy_limited(proxy)
                else:
                    self.record_err(f"HTTP {response.status_code}")
                    print(
                        f"    [ {self.grn}$ {self.white}]{Fore.RED} Unexpected status {self.white}{response.status_code}{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                    self.proxy_failed(proxy, f"HTTP {response.status_code}")
                    time.sleep(random.uniform(1, 2))
            except requests.exceptions.ChunkedEncodingError:
                self.record_err("response cut")
                if proxy is not None:
                    self.proxy_failed(proxy, "ChunkedEncodingError")
                if proxyDebug:
                    print(f"    {Fore.RED}ChunkedEncodingError: {proxy}{Fore.RESET}")
                continue
            except requests.exceptions.RequestException as e:
                reason = str(e)
                if "Caused by" in reason:
                    reason = reason.split("Caused by", 1)[1]
                reason = " ".join(reason.split())[:200]
                self.record_err(error_kind(e))
                if proxy is not None:
                    # Individual errors only appear with proxyDebug=True; the proxy is removed after MAX_PROXY_FAILS in a row
                    self.proxy_failed(proxy, reason)
                    if proxyDebug:
                        kind = "Proxy down" if isinstance(e, (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout)) else "Request Error"
                        print(f"    {Fore.RED}{kind}: {proxy} -> {reason}{Fore.RESET}")
                continue
            except KeyboardInterrupt: 
                print(f"    [ {Fore.RED}$ {self.white}]{Fore.RED} Exit ...")
                exit()
            finally:
                if response is not None:
                    try:
                        response.close()
                    except Exception:
                        pass
                if proxy is not None:
                    self.release_proxy(proxy)

if __name__ == "__main__":
    Checker()
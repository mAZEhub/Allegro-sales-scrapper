import pandas as pd
import requests
import time
import re
from tqdm import tqdm
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

INPUT_FILE = "allegro_enriched_full.csv"
OUTPUT_FILE = "allegro_after_search.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

BAD_DOMAINS = [
    "parenting.pl",
    "atlasimion.pl",
    "imiona.com.pl",
    "reddit.com",
    "forum",
    "o2.pl",
    "pudelek",
    "plotek",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "community.ebay",
    "zhihu",
    "imdb",
    "tb-manual.torproject.org",
    "baidu.com",
    "espn",
    "everymature",
    "google.com",
    "google.pl",
    "wikipedia.org"
]


# ===============================
# CLEAN STRING
# ===============================

def clean_name(name):
    if not isinstance(name, str):
        return ""
    name = name.lower()
    name = re.sub(r"sp\.?\s*z\.?\s*o\.?o\.?", "", name)
    name = re.sub(r"s\.?c\.?", "", name)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def extract_domain(url):
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/"
    except:
        return None


# ===============================
# WYBÓR NAJLEPSZEJ NAZWY FIRMY
# ===============================

def choose_best_company_name(row):
    possible = []

    if "company_name" in row and isinstance(row["company_name"], str):
        possible.append(row["company_name"])

    if "owner_name" in row and isinstance(row["owner_name"], str):
        possible.append(row["owner_name"])

    if "login" in row and isinstance(row["login"], str):
        possible.append(row["login"])

    possible = [p for p in possible if len(p) > 4]

    if not possible:
        return ""

    return max(possible, key=len)


# ===============================
# SCORING
# ===============================

def score_domain(link, company, login, address, nip):
    score = 0
    domain = urlparse(link).netloc.lower()

    company_clean = clean_name(company)
    login_clean = clean_name(login)

    if any(bad in domain for bad in BAD_DOMAINS):
        return -100

    if company_clean and company_clean in domain:
        score += 50

    if login_clean and login_clean in domain:
        score += 30

    if ".pl" in domain:
        score += 20

    if nip and nip[:5] in link:
        score += 10

    if address:
        address_parts = address.lower().split()
        for part in address_parts:
            if len(part) > 4 and part in domain:
                score += 10

    if any(x in domain for x in ["forum", "blog", "news"]):
        score -= 20

    if any(x in domain for x in [".cn", ".ru", ".in", ".ec"]):
        score -= 30

    return score


# ===============================
# BING RSS
# ===============================

def bing_search(query):
    print(f"   🌍 Bing RSS: {query}")
    url = f"https://www.bing.com/search?q={query}&format=rss"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        root = ET.fromstring(r.content)

        results = []
        for item in root.findall(".//item"):
            link = item.find("link").text
            results.append(link)

        print(f"   🔎 Realne wyniki (RSS): {len(results)}")
        return results

    except Exception as e:
        print("   ❌ Bing RSS błąd:", e)
        return []


# ===============================
# HEURYSTYKA
# ===============================

def heuristic_domain(company):
    clean = clean_name(company)
    if not clean:
        return None

    for ext in [".pl", ".com", ".eu"]:
        test = f"https://{clean}{ext}"
        try:
            r = requests.get(test, headers=HEADERS, timeout=5)
            if r.status_code == 200:
                print(f"   ✅ Heurystyka trafiła: {test}")
                return test
        except:
            continue

    return None


# ===============================
# BUDOWANIE ZAPYTAŃ
# ===============================

def build_queries(company, address, nip, login):
    queries = []

    if company:
        queries.append(f"{company} {address}")
        queries.append(f"{company} Polska")
        queries.append(f"{company} {nip}")

    if login:
        queries.append(f"{login} sklep internetowy")

    return list(set([q for q in queries if q]))


# ===============================
# GŁÓWNY ENRICH
# ===============================

def enrich():
    df = pd.read_csv(INPUT_FILE, encoding="utf-8")

    if "website" not in df.columns:
        df["website"] = ""
        df["source"] = ""
        df["confidence"] = 0

    for i in tqdm(range(len(df))):

        if pd.notna(df.loc[i, "website"]) and df.loc[i, "website"] != "":
            continue

        print("\n==============================")
        print(f"🔎 Rekord {i}")

        row = df.loc[i]

        company = choose_best_company_name(row)
        address = str(row["address"]) if "address" in df.columns else ""
        nip = str(row["nip"]) if "nip" in df.columns else ""
        login = str(row["login"]) if "login" in df.columns else ""

        print(f"   🏷 Szukam: {company}")

        queries = build_queries(company, address, nip, login)
        all_results = []

        for q in queries:
            results = bing_search(q)
            all_results.extend(results)
            time.sleep(1)

        best_score = -999
        chosen = None

        for link in all_results:
            s = score_domain(link, company, login, address, nip)
            print(f"      🔎 {link} → SCORE {s}")

            if s > best_score:
                best_score = s
                chosen = link

        if chosen and best_score > 0:
            chosen = extract_domain(chosen)
            print(f"   🌍 WYBRANA DOMENA (score {best_score}): {chosen}")
        else:
            print("   🔍 Próbuję heurystyki...")
            chosen = heuristic_domain(company)

        if not chosen:
            print("   ❌ Nie znaleziono domeny")
            continue

        df.loc[i, "website"] = chosen
        df.loc[i, "source"] = "bing_rss_pro"
        df.loc[i, "confidence"] = best_score

        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print("\n✅ GOTOWE")


if __name__ == "__main__":
    enrich()
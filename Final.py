import csv
import re
import time
import os
import threading
import random
from queue import Queue
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

INPUT_FILE = "automotive_sellers_categories.csv"
OUTPUT_FILE = "allegro_enriched_full.csv"

MAX_DRIVERS = 3
START_DELAY = 35
RESTART_AFTER = 25
MAX_RETRIES = 3

queue = Queue()
lock = threading.Lock()


# ===============================
# BLOCK DETECTION
# ===============================

def is_blocked(driver):
    try:
        current_url = driver.current_url.lower()
        body = driver.page_source.lower()

        if "verify" in current_url:
            return True

        if "captcha" in current_url:
            return True

        if "zostałeś zablokowany" in body:
            return True

        if "automatyczne zapytania" in body:
            return True

        return False

    except:
        return True


# ===============================
# DRIVER
# ===============================

def create_driver(profile_id):

    options = Options()

    profile_path = os.path.join(os.getcwd(), f"chrome_profile_{profile_id}")

    options.add_argument(f"--user-data-dir={profile_path}")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver


def restart_driver(driver, worker_id):
    try:
        driver.quit()
    except:
        pass

    print(f"🔄 Driver {worker_id} RESTART")
    time.sleep(random.uniform(8, 15))
    return create_driver(worker_id)


# ===============================
# COMPLETE CHECK
# ===============================

def is_complete(row):
    return (
        row.get("company_name")
        and (row.get("nip") or row.get("regon") or row.get("krs"))
    )


# ===============================
# COMPANY EXTRACTION
# ===============================

def extract_company(text):

    data = {
        "owner_name": "",
        "company_name": "",
        "address": "",
        "nip": "",
        "regon": "",
        "krs": "",
        "email": "",
        "phone": "",
        "bank_account": "",
        "entity_type": ""
    }

    if "Dane firmy" in text:
        section = text.split("Dane firmy")[1]
        lines = [l.strip() for l in section.split("\n") if l.strip()]

        if len(lines) >= 1:
            data["owner_name"] = lines[0]
        if len(lines) >= 2:
            data["company_name"] = lines[1]
        if len(lines) >= 4:
            data["address"] = lines[2] + " " + lines[3]

    patterns = {
        "nip": r"NIP[:\s\-]*([0-9]{10})",
        "regon": r"REGON[:\s\-]*([0-9]{9,14})",
        "krs": r"KRS[:\s\-]*([0-9]{10})"
    }

    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            data[key] = m.group(1)

    emails = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)
    if emails:
        data["email"] = emails[0]

    phone = re.search(r"\+?48?\s?\d{3}\s?\d{3}\s?\d{3}", text)
    if phone:
        data["phone"] = phone.group(0)

    bank = re.search(r"\b\d{2}\s?\d{4}\s?\d{4}\s?\d{4}\s?\d{4}\s?\d{4}\s?\d{4}\b", text)
    if bank:
        data["bank_account"] = bank.group(0)

    if "sp. z o.o" in text.lower():
        data["entity_type"] = "SP. Z O.O."
    elif data["nip"] and not data["krs"]:
        data["entity_type"] = "JDG"
    else:
        data["entity_type"] = "INNY"

    return data


# ===============================
# RATING EXTRACTION
# ===============================

def extract_rating(driver):

    data = {
        "recommendation_percent": 0,
        "positive_count": 0,
        "negative_count": 0,
        "neutral_count": 0,
        "total_feedback": 0,
        "super_seller": False,
        "years_on_allegro": 0,
        "total_offers": 0,
        "categories_count": 0
    }

    body = driver.find_element(By.TAG_NAME, "body").text

    percent = re.search(r"(\d{1,3})%", body)
    if percent:
        data["recommendation_percent"] = int(percent.group(1))

    if "Super sprzedawca" in body:
        data["super_seller"] = True

    years = re.search(r"od (\d+) lat", body)
    if years:
        data["years_on_allegro"] = int(years.group(1))

    offers = driver.find_elements(By.CSS_SELECTOR, "a[href*='/oferta/']")
    data["total_offers"] = len(offers)

    categories = re.findall(r"z kategorii", body)
    data["categories_count"] = len(categories)

    return data


# ===============================
# SCORING
# ===============================

def calculate_score(data):

    score = 0

    if data["super_seller"]:
        score += 30

    if data["recommendation_percent"] >= 99:
        score += 20

    if data["total_offers"] >= 200:
        score += 15

    if data["years_on_allegro"] >= 5:
        score += 10

    if data["entity_type"] == "SP. Z O.O.":
        score += 10

    if data["email"]:
        score += 10

    segment = "C"
    if score >= 75:
        segment = "A"
    elif score >= 50:
        segment = "B"

    data["seller_score"] = score
    data["segment"] = segment

    return data


# ===============================
# LOAD EXISTING
# ===============================

existing_complete = set()

if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if is_complete(r):
                existing_complete.add(r["login"])

print("✔ Kompletnych:", len(existing_complete))


rows = []

with open(INPUT_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames + [
        "owner_name","company_name","address","nip","regon","krs","email","phone","bank_account","entity_type",
        "recommendation_percent","positive_count","negative_count","neutral_count","total_feedback",
        "super_seller","years_on_allegro","total_offers","categories_count",
        "seller_score","segment"
    ]

    for r in reader:
        if r["login"] not in existing_complete:
            rows.append(r)
            queue.put(r)

print("🚀 Do przetworzenia:", len(rows))


# ===============================
# WORKER
# ===============================

def worker(worker_id):

    driver = create_driver(worker_id)
    print(f"🟢 Driver {worker_id} start")
    counter = 0

    while not queue.empty():

        row = queue.get()
        login = row["login"]

        retries = 0

        while retries < MAX_RETRIES:

            try:
                print(f"\n🔎 Driver {worker_id} | {login}")

                driver.get(f"https://allegro.pl/uzytkownik/{login}")
                time.sleep(random.uniform(4, 7))

                if is_blocked(driver):
                    raise Exception("BLOCKED")

                offers = driver.find_elements(By.CSS_SELECTOR, "a[href*='/oferta/']")

                if not offers:

                    if is_blocked(driver):
                        raise Exception("BLOCKED")

                    print(f"ℹ {login} realnie brak ofert")
                    break

                first_offer = offers[0].get_attribute("href")
                driver.get(first_offer + "#about-seller")
                time.sleep(random.uniform(4, 7))

                if is_blocked(driver):
                    raise Exception("BLOCKED")

                body_text = driver.find_element(By.TAG_NAME, "body").text

                company_data = extract_company(body_text)
                rating_data = extract_rating(driver)

                merged = {**row, **company_data, **rating_data}
                merged = calculate_score(merged)

                with lock:
                    writer.writerow(merged)
                    out_file.flush()

                print(f"   ✔ {login} → {merged['segment']} ({merged['seller_score']} pkt)")

                counter += 1

                if counter >= RESTART_AFTER:
                    driver = restart_driver(driver, worker_id)
                    counter = 0

                break

            except Exception as e:

                if "BLOCKED" in str(e):
                    print(f"🚨 {login} BLOCKED")
                    driver = restart_driver(driver, worker_id)
                    retries += 1
                else:
                    print(f"⚠ {login} błąd: {e}")
                    break

    driver.quit()


file_exists = os.path.exists(OUTPUT_FILE)

with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as out_file:
    writer = csv.DictWriter(out_file, fieldnames=fieldnames)

    if not file_exists:
        writer.writeheader()

    threads = []

    for i in range(MAX_DRIVERS):
        t = threading.Thread(target=worker, args=(i+1,))
        t.start()
        threads.append(t)
        time.sleep(START_DELAY)

    for t in threads:
        t.join()

print("\n🎯 DONE")
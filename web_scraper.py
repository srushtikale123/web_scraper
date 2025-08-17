import csv
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ----------------------------- Configuration ----------------------------- #
QUOTE_SELECTOR = ("span", {"class": "text"})
AUTHOR_SELECTOR = ("small", {"class": "author"})
NEXT_PAGE_STRATEGIES = [
    {"css": ("li", {"class": "next"}), "child": ("a", {})},
    {"css": ("a", {"rel": "next"}), "child": None},
]

DEFAULT_URL = "https://quotes.toscrape.com"
DEFAULT_CSV = "scraped_data.csv"
DEFAULT_JSON = "scraped_data.json"
DEFAULT_DB = "scraped_data.sqlite3"
DEFAULT_SUMMARY = "scrape_summary.json"

REQUEST_TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (compatible; EducationalScraper/1.0; +https://example.com)"


# ------------------------------ Data Models ------------------------------ #
@dataclass(frozen=True)
class QuoteItem:
    quote: str
    author: str

    def key(self) -> Tuple[str, str]:
        return (self.quote, self.author)


# ---------------------------- Utility Functions -------------------------- #
def is_valid_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return bool(parsed.scheme) and bool(parsed.netloc)
    except Exception:
        return False


def http_get(url: str) -> Optional[requests.Response]:
    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except requests.exceptions.RequestException as e:
        print(f"❌ HTTP error for {url}: {e}")
        return None


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    s = re.sub(r"\s+", " ", s)
    return s


# --------------------------- Parsing / Extraction ------------------------ #
def extract_items_from_soup(soup: BeautifulSoup) -> List[QuoteItem]:
    quotes = soup.find_all(*QUOTE_SELECTOR)
    authors = soup.find_all(*AUTHOR_SELECTOR)

    items: List[QuoteItem] = []
    for q_tag, a_tag in zip(quotes, authors):
        q = clean_text(q_tag.get_text())
        a = clean_text(a_tag.get_text())
        if q and a:
            items.append(QuoteItem(quote=q, author=a))
    return items


def find_next_page_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    for strat in NEXT_PAGE_STRATEGIES:
        parent_sel = strat["css"]
        child_sel = strat["child"]

        parent = soup.find(*parent_sel)
        if not parent:
            continue

        node = parent.find(*child_sel) if child_sel else parent
        href = node.get("href") if node else None
        if href:
            return urljoin(base_url, href)
    return None


# ------------------------------ Scrape Routines -------------------------- #
def scrape_single_page(url: str) -> List[QuoteItem]:
    resp = http_get(url)
    if resp is None:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    return extract_items_from_soup(soup)


def scrape_with_pagination(start_url: str, max_pages: Optional[int] = None) -> List[QuoteItem]:
    visited = set()
    results: List[QuoteItem] = []
    seen_keys = set()

    url = start_url
    pages = 0

    while url and url not in visited:
        print(f"🔎 Scraping: {url}")
        visited.add(url)

        resp = http_get(url)
        if resp is None:
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        items = extract_items_from_soup(soup)
        for item in items:
            k = item.key()
            if k not in seen_keys:
                results.append(item)
                seen_keys.add(k)

        pages += 1
        if max_pages is not None and pages >= max_pages:
            break

        url = find_next_page_url(soup, url)

    return results


# ------------------------------ Storage I/O ------------------------------ #
def save_to_csv(items: List[QuoteItem], path: str = DEFAULT_CSV) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["quote", "author"])
        writer.writeheader()
        for it in items:
            writer.writerow({"quote": it.quote, "author": it.author})
    print(f"📄 CSV saved: {path}")


def save_to_json(items: List[QuoteItem], path: str = DEFAULT_JSON) -> None:
    data = [{"quote": it.quote, "author": it.author} for it in items]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"🧾 JSON saved: {path}")


def save_to_sqlite(items: List[QuoteItem], db_path: str = DEFAULT_DB, table: str = "quotes") -> None:
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote TEXT NOT NULL,
                author TEXT NOT NULL,
                UNIQUE(quote, author)
            )
            """
        )
        con.commit()

        cur.executemany(
            f"INSERT OR IGNORE INTO {table} (quote, author) VALUES (?, ?)",
            [(it.quote, it.author) for it in items],
        )
        con.commit()

        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total = cur.fetchone()[0]
        print(f"🗃 SQLite saved to '{db_path}' table '{table}'. Total rows now: {total}")
    finally:
        con.close()


# ------------------------------ Reporting -------------------------------- #
def export_summary(items: List[QuoteItem], path: str = DEFAULT_SUMMARY) -> Dict:
    total = len(items)
    unique_authors = len({it.author for it in items})
    summary = {
        "total_items": total,
        "unique_authors": unique_authors,
        "sample": [{"quote": it.quote, "author": it.author} for it in items[:5]],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print("📊 Summary:", json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"📝 Summary saved: {path}")
    return summary


# --------------------------------- Main ---------------------------------- #
def main():
    print("🔹 Python Web Scraper (with required + optional features)")
    url = input(f"Enter start URL (default: {DEFAULT_URL}): ").strip() or DEFAULT_URL

    if not is_valid_url(url):
        print("❌ Invalid URL. Please include scheme (http/https) and domain.")
        sys.exit(1)

    use_pagination = input("Use pagination? [y/N]: ").strip().lower() == "y"
    max_pages = None
    if use_pagination:
        raw = input("Max pages to crawl (Enter for no limit): ").strip()
        if raw.isdigit():
            max_pages = int(raw)

    if use_pagination:
        items = scrape_with_pagination(url, max_pages=max_pages)
    else:
        items = scrape_single_page(url)

    if not items:
        print("⚠ No data scraped.")
        sys.exit(0)

    print(f"✅ Scraped {len(items)} items.")

    save_to_csv(items, DEFAULT_CSV)
    save_to_json(items, DEFAULT_JSON)

    if input("Store to SQLite? [y/N]: ").strip().lower() == "y":
        save_to_sqlite(items, DEFAULT_DB, table="quotes")

    export_summary(items, DEFAULT_SUMMARY)

    print("🎉 Done.")


if __name__ == "__main__":
    main()

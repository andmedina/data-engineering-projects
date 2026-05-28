#!/usr/bin/env python3
"""
BeautifulSoup Practice Lab.

This script demonstrates foundational web scraping concepts using
BeautifulSoup and requests.

Topics covered:
- BeautifulSoup object structure (tags, parents, children, siblings)
- HTML attributes and NavigableString objects
- Searching and filtering with find() and find_all()
- Extracting links and images from live webpages
- Scraping structured data from HTML tables

This file is intended for learning and experimentation rather than
production data pipelines.
"""

from __future__ import annotations

import re
from typing import Any

import requests
from bs4 import BeautifulSoup


def step(message: str) -> None:
    """Print a lightweight progress message."""
    print(f"[STEP] {message}")


def build_sample_soup() -> BeautifulSoup:
    """Create a BeautifulSoup object from a small sample HTML string."""
    html = (
        "<!DOCTYPE html><html><head><title>Page Title</title></head><body><h3>"
        "<b id='boldest'>Lebron James</b></h3><p> Salary: $ 92,000,000 </p>"
        "<h3>Stephen Curry</h3><p> Salary: $85,000,000</p>"
        "<h3>Kevin Durant</h3><p> Salary: $73,200,000</p></body></html>"
    )
    return BeautifulSoup(html, "html5lib")


def remove_citations(text: Any) -> Any:
    """
    Remove Wikipedia-style citation markers like [1], [2], etc.

    Parameters
    ----------
    text : Any
        Any value (string values will be cleaned; non-strings returned unchanged).

    Returns
    -------
    Any
        Cleaned string or original value if not a string.
    """
    if isinstance(text, str):
        return re.sub(r"\[.*?\]", "", text)
    return text


def fetch_html(url: str, timeout: int = 30) -> str:
    """
    Fetch HTML content from a URL using a browser-like User-Agent.

    Raises
    ------
    requests.HTTPError
        If the response status is not 2xx.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text


def print_all_hrefs(url: str, limit: int = 25) -> None:
    """Download a page and print up to `limit` link hrefs found on it."""
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html5lib")
    links = [a.get("href") for a in soup.find_all("a", href=True)]

    step(f"Found {len(links)} links on {url}")
    for href in links[:limit]:
        print(href)

    if len(links) > limit:
        print(f"... (showing {limit} of {len(links)})")


def print_all_image_src(url: str, limit: int = 25) -> None:
    """Download a page and print up to `limit` image src attributes found on it."""
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html5lib")
    srcs = [img.get("src") for img in soup.find_all("img") if img.get("src")]

    step(f"Found {len(srcs)} images on {url}")
    for src in srcs[:limit]:
        print(src)

    if len(srcs) > limit:
        print(f"... (showing {limit} of {len(srcs)})")


def scrape_color_table(url: str, limit: int = 20) -> None:
    """
    Scrape the color table from the IBM Skills Network HTMLColorCodes page.

    Prints: color_name--->color_code
    """
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html5lib")

    table = soup.find("table")
    if table is None:
        raise ValueError("No <table> found on the page.")

    printed = 0
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        color_name = cols[2].get_text(strip=True)
        color_code = cols[3].get_text(strip=True)
        print(f"{color_name}--->{color_code}")
        printed += 1

        if printed >= limit:
            break

    step(f"Printed {printed} color rows (limit={limit})")


def demo_find_all_and_find() -> None:
    """Demonstrate find_all() and find() usage on sample HTML snippets."""
    table_html = (
        "<table><tr><td id='flight'>Flight No</td><td>Launch site</td>"
        "<td>Payload mass</td></tr><tr><td>1</td>"
        "<td><a href='https://en.wikipedia.org/wiki/Florida'>Florida</a></td>"
        "<td>300 kg</td></tr><tr><td>2</td>"
        "<td><a href='https://en.wikipedia.org/wiki/Texas'>Texas</a></td>"
        "<td>94 kg</td></tr><tr><td>3</td>"
        "<td><a href='https://en.wikipedia.org/wiki/Florida'>Florida</a></td>"
        "<td>80 kg</td></tr></table>"
    )
    table_soup = BeautifulSoup(table_html, "html5lib")

    table_rows = table_soup.find_all("tr")
    step(f"Rows found in demo table: {len(table_rows)}")

    ids = table_soup.find_all(id="flight")
    step(f"Elements with id='flight': {len(ids)}")

    anchors = table_soup.find_all("a", href=True)
    step(f"Anchors with href: {len(anchors)}")

    fl_strings = table_soup.find_all(string="Florida")
    step(f"Strings exactly 'Florida': {len(fl_strings)}")


def demo_bs4_object_navigation() -> None:
    """Demonstrate tags, parents/children/siblings, attributes, and strings."""
    soup = build_sample_soup()

    title_tag = soup.title
    step(f"Title tag text: {title_tag.string if title_tag else None}")

    first_h3 = soup.h3
    step(f"First <h3>: {first_h3.get_text(strip=True) if first_h3 else None}")

    bold_child = first_h3.b if first_h3 else None
    if bold_child is None:
        step("No <b> child found under first <h3>")
        return

    step(f"Bold child id: {bold_child.get('id')}")
    step(f"Bold child string: {bold_child.string}")


def main() -> None:
    """Run a few demonstrations from the lab."""
    step("Starting BeautifulSoup lab demo")

    step("Running object navigation demo")
    demo_bs4_object_navigation()

    step("Running find_all/find demo")
    demo_find_all_and_find()

    # Real-world scraping demos (optional network calls)
    step("Running live scrape demos (network)")
    print_all_hrefs("https://www.ibm.com", limit=15)
    print_all_image_src("https://www.ibm.com", limit=10)

    color_url = (
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
        "IBM-DA0321EN-SkillsNetwork/labs/datasets/HTMLColorCodes.html"
    )
    scrape_color_table(color_url, limit=15)

    step("Done")


if __name__ == "__main__":
    main()

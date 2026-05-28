#!/usr/bin/env python3
"""
Wikipedia scraping example using BeautifulSoup.

This script:
- Sends an HTTP request to a Wikipedia page
- Parses HTML content using BeautifulSoup
- Extracts and prints all anchor tag text
"""

import requests
from bs4 import BeautifulSoup

URL = "https://en.wikipedia.org/wiki/IBM"


def fetch_html(url: str) -> str:
    """Fetch HTML content from a webpage."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def parse_html(html: str) -> BeautifulSoup:
    """Create BeautifulSoup parser object."""
    return BeautifulSoup(html, "html.parser")


def extract_links(soup: BeautifulSoup) -> None:
    """Print text from all anchor tags."""
    links = soup.find_all("a")

    for link in links:
        text = link.get_text(strip=True)
        if text:
            print(text)


def main() -> None:
    """Run scraping workflow."""
    html_content = fetch_html(URL)

    # preview HTML snippet
    print(html_content[:500])

    soup = parse_html(html_content)
    extract_links(soup)


if __name__ == "__main__":
    main()

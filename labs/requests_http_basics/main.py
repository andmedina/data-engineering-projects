#!/usr/bin/env python3
"""
Web Requests Basics Lab.

Demonstrates:
- Basic GET requests and inspecting request/response metadata
- Downloading binary (image) and text files
- Sending query parameters with GET
- Sending form data with POST (httpbin)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import requests


USER_AGENT = "Mozilla/5.0 (compatible; requests-basics-lab/1.0)"


def get_session() -> requests.Session:
    """Create a requests session with a default User-Agent header."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_url(session: requests.Session, url: str, timeout: int = 30) -> requests.Response:
    """Fetch a URL and raise an exception for HTTP errors."""
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response


def download_file(session: requests.Session, url: str, out_path: Path, timeout: int = 30) -> None:
    """Download a URL to a local file (binary-safe)."""
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(response.content)


def get_with_params(session: requests.Session, url: str, params: dict[str, str]) -> dict[str, Any]:
    """Send a GET request with query parameters and return parsed JSON."""
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def post_form(session: requests.Session, url: str, data: dict[str, str]) -> dict[str, Any]:
    """Send a POST request with form data and return parsed JSON."""
    response = session.post(url, data=data, timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> None:
    """Run the lab examples."""
    session = get_session()

    # 1) Simple GET to IBM homepage (metadata inspection)
    url_home = "https://www.ibm.com/"
    resp = fetch_url(session, url_home)
    print("GET:", url_home)
    print("Status:", resp.status_code)
    print("Request headers:", dict(resp.request.headers))
    print("Request body:", resp.request.body)
    print("Response Content-Type:", resp.headers.get("Content-Type"))
    print("Response Date:", resp.headers.get("Date"))
    print("Encoding:", resp.encoding)
    print("HTML snippet:", resp.text[:120], "\n")

    # 2) Download an image (binary)
    image_url = (
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
        "IBMDeveloperSkillsNetwork-PY0101EN-SkillsNetwork/IDSNlogo.png"
    )
    image_path = Path("output") / "image.png"
    download_file(session, image_url, image_path)
    print("Downloaded image to:", image_path)

    # 3) Download a text file
    text_url = (
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
        "IBMDeveloperSkillsNetwork-PY0101EN-SkillsNetwork/labs/Module%205/data/Example1.txt"
    )
    text_path = Path("output") / "example1.txt"
    download_file(session, text_url, text_path)
    print("Downloaded text to:", text_path, "\n")

    # 4) GET with query parameters
    url_get = "https://httpbin.org/get"
    payload = {"name": "Joseph", "ID": "123"}
    get_json = get_with_params(session, url_get, payload)
    print("GET with params URL:", get_json.get("url"))
    print("GET args returned:", get_json.get("args"), "\n")

    # 5) POST with form data
    url_post = "https://httpbin.org/post"
    post_json = post_form(session, url_post, payload)
    print("POST form returned:", post_json.get("form"))


if __name__ == "__main__":
    main()

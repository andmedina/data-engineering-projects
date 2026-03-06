#!/usr/bin/env python3
"""
Shell-style ETL demo: /etc/passwd -> SQLite.

This script demonstrates an ETL-style pipeline using classic shell tools:
- Extract: cut fields (username, uid, home) from /etc/passwd
- Transform: replace delimiters and drop header/system rows
- Load: import CSV into SQLite and run a sample query

Notes:
- This is intentionally "shell flavored" (cut/tr/sed) but runs as a normal Python script.
- Requires: cut, tr, sed, sqlite3 available on your system.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    """Paths used by the pipeline."""
    work_dir: Path
    extracted_txt: Path
    transformed_csv: Path
    cleaned_csv: Path
    db_path: Path


def run_cmd(cmd: list[str], *, stdin_text: str | None = None) -> str:
    """
    Run a command and return stdout.

    Parameters
    ----------
    cmd : list[str]
        Command and args, e.g. ["cut", "-d:", "-f1,3,6", "/etc/passwd"].
    stdin_text : str | None
        Optional stdin text to send to the process.

    Returns
    -------
    str
        Stdout from the command.

    Raises
    ------
    subprocess.CalledProcessError
        If the command exits with a non-zero status.
    """
    result = subprocess.run(
        cmd,
        input=stdin_text,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def ensure_tools_exist() -> None:
    """Fail fast if required CLI tools are missing."""
    required = ["cut", "tr", "sed", "sqlite3"]
    missing = [tool for tool in required if not shutil_which(tool)]
    if missing:
        raise RuntimeError(f"Missing required CLI tools: {', '.join(missing)}")


def shutil_which(cmd: str) -> str | None:
    """Minimal 'which' implementation (avoids importing shutil if you prefer)."""
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(path) / cmd
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def init_paths() -> Paths:
    """Create output folder and return standard output file paths."""
    work_dir = Path.cwd()
    return Paths(
        work_dir=work_dir,
        extracted_txt=work_dir / "extracted-data.txt",
        transformed_csv=work_dir / "transformed-data.csv",
        cleaned_csv=work_dir / "cleaned-data.csv",
        db_path=work_dir / "database.db",
    )


def create_table(db_path: Path, table_name: str = "template1") -> None:
    """
    Create the destination table in SQLite.

    Parameters
    ----------
    db_path : Path
        SQLite database file path.
    table_name : str
        Table name to create.
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        username TEXT,
        userid INTEGER,
        homedirectory TEXT
    );
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        conn.execute(create_sql)
        conn.commit()


def extract_passwd(paths: Paths) -> None:
    """
    Extract username, uid, and home directory from /etc/passwd.

    Output format: colon-delimited lines like:
    username:userid:/home/username
    """
    output = run_cmd(["cut", "-d:", "-f1,3,6", "/etc/passwd"])
    paths.extracted_txt.write_text(output, encoding="utf-8")


def transform_to_csv(paths: Paths, *, drop_first_lines: int = 9) -> None:
    """
    Transform extracted data to CSV.

    Steps:
    - Replace ':' with ','
    - Drop the first N lines (often system accounts; matches notebook intent)

    Parameters
    ----------
    drop_first_lines : int
        Number of lines to drop from the top of the file.
    """
    extracted = paths.extracted_txt.read_text(encoding="utf-8")

    replaced = run_cmd(["tr", ":", ","], stdin_text=extracted)
    paths.transformed_csv.write_text(replaced, encoding="utf-8")

    # sed '1,9d' means delete lines 1 through 9
    sed_expr = f"1,{drop_first_lines}d"
    cleaned = run_cmd(["sed", sed_expr, str(paths.transformed_csv)])
    paths.cleaned_csv.write_text(cleaned, encoding="utf-8")


def load_into_sqlite(paths: Paths, table_name: str = "template1") -> None:
    """
    Import cleaned CSV into SQLite using sqlite3 CLI.

    This keeps the workflow close to the notebook demo.
    """
    sql = "\n".join(
        [
            ".mode csv",
            f".import {paths.cleaned_csv} {table_name}",
        ]
    ) + "\n"
    _ = run_cmd(["sqlite3", str(paths.db_path)], stdin_text=sql)


def print_sample_query(paths: Paths, table_name: str = "template1") -> None:
    """Print a sample query result to prove the pipeline worked."""
    query = f"SELECT * FROM {table_name} LIMIT 15;"
    output = run_cmd(["sqlite3", "-header", "-column", str(paths.db_path), query])
    print(output)


def main() -> None:
    """Run the ETL demo pipeline."""
    # ensure_tools_exist()  # uncomment if you want strict checks
    paths = init_paths()

    create_table(paths.db_path)
    extract_passwd(paths)
    transform_to_csv(paths)
    load_into_sqlite(paths)
    print_sample_query(paths)


if __name__ == "__main__":
    main()

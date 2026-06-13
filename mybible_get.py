#!/usr/bin/env python3
import click
import requests
import sqlite3
import json
import zipfile
import io
import os
import sys
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from functools import wraps
import operator
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.markup import escape
from rich.box import ROUNDED
from rich.rule import Rule
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# --- CONFIGURATION ---
APP_NAME = "mybible-get"

def get_os_config_dir():
    if os.name == 'nt':
        return Path(os.getenv('APPDATA')) / APP_NAME
    elif sys.platform == 'darwin':
        return Path.home() / 'Library' / 'Application Support' / APP_NAME
    else:
        return Path.home() / '.config' / APP_NAME

CONFIG_DIR = get_os_config_dir()
SOURCES_DIR = CONFIG_DIR / "sources"
CACHE_DIR = CONFIG_DIR / ".cache"
DOWNLOAD_CACHE_DIR = CACHE_DIR / "downloads"
REGISTRY_CACHE_DIR = CACHE_DIR / "registries"
CACHE_DB_PATH = CACHE_DIR / "cache.db"
CONFIG_FILE_PATH = CONFIG_DIR / "config.json"
ETAG_CACHE_PATH = CONFIG_DIR / "etags.json"

DEFAULT_SOURCES = {
    "mybible.zone.registry": "https://mybible.zone/repository/registry/registry.zip",
    "myb.1gb.ru.registry": "http://myb.1gb.ru/registry.zip",
    "mybible.infoo.pro.registry": "http://mybible.infoo.pro/registry.zip",
    "mph4.ru.registry": "http://mph4.ru/registry.zip",
    "dropbox.registry": "https://dl.dropbox.com/s/keg0ptkkalux5fi/registry.zip",
    "mph4_test.registry": "http://mph4.ru/registry_test.zip",
    "myb.1gb.ru_test.registry": "http://myb.1gb.ru/registry_test.zip",
    "mybible.zone_test.registry": "https://mybible.zone/repository/registry/registry_test.zip",
}

console = Console()

# ---------------------------------------------------------------------------
# Cache DB  (CACHE_DIR/cache.db)
# Table: cached_modules  — identical schema to mybible-cli-java
# ---------------------------------------------------------------------------

def _cache_conn():
    CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(CACHE_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def ensure_cache_db():
    with _cache_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS cached_modules (
                name            TEXT NOT NULL,
                language        TEXT,
                description     TEXT NOT NULL,
                update_date     TEXT NOT NULL,
                download_url    TEXT NOT NULL,
                file_name       TEXT NOT NULL,
                module_type     TEXT NOT NULL,
                size            TEXT,
                source_registry TEXT NOT NULL,
                PRIMARY KEY (name, update_date, download_url)
            );
            CREATE INDEX IF NOT EXISTS idx_module_name ON cached_modules(name);
            CREATE INDEX IF NOT EXISTS idx_module_type ON cached_modules(module_type);
            CREATE INDEX IF NOT EXISTS idx_language    ON cached_modules(language);
        """)

def is_cache_empty():
    if not CACHE_DB_PATH.exists():
        return True
    try:
        ensure_cache_db()
        with _cache_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM cached_modules").fetchone()[0] == 0
    except sqlite3.Error:
        return True

def _cache_insert_many(modules):
    with _cache_conn() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO cached_modules "
            "(name,language,description,update_date,download_url,file_name,module_type,size,source_registry) "
            "VALUES (:name,:language,:description,:update_date,:download_url,:file_name,:module_type,:size,:source_registry)",
            modules,
        )

def _cache_get_module(name, version=None):
    sql = "SELECT * FROM cached_modules WHERE LOWER(name) LIKE LOWER(?)"
    params = [name]
    if version:
        sql += " AND update_date = ?"
        params.append(version)
    sql += " ORDER BY update_date DESC LIMIT 1"
    with _cache_conn() as conn:
        return conn.execute(sql, params).fetchone()

def _cache_list(language=None, module_type=None):
    sql = "SELECT * FROM cached_modules WHERE 1=1"
    params = []
    if language:
        sql += " AND LOWER(language) LIKE LOWER(?)"
        params.append(f"%{language}%")
    if module_type:
        sql += " AND LOWER(module_type) LIKE LOWER(?)"
        params.append(f"%{module_type}%")
    sql += " GROUP BY LOWER(name) ORDER BY LOWER(name)"
    with _cache_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]

def _cache_versions(name):
    with _cache_conn() as conn:
        return conn.execute(
            "SELECT DISTINCT update_date FROM cached_modules "
            "WHERE LOWER(name) LIKE LOWER(?) ORDER BY update_date DESC",
            [name],
        ).fetchall()

# ---------------------------------------------------------------------------
# Installed DB  (<module_path>/mybible_installed.db)
# Schema identical to mybible-cli-java:
#   installed_modules(name PK, language, description, type, updatedate, installdate)
#   installed_files(id PK AUTOINCREMENT, module_name FK, file_name, file_path)
# ---------------------------------------------------------------------------

def _installed_db_path():
    mp = get_module_path()
    return Path(mp) / "mybible_installed.db" if mp else None

def _inst_conn():
    p = _installed_db_path()
    if p is None:
        raise RuntimeError("Module path not set. Use 'set-path' first.")
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def ensure_installed_db():
    if _installed_db_path() is None:
        return
    with _inst_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS installed_modules (
                name        TEXT PRIMARY KEY,
                language    TEXT,
                description TEXT NOT NULL,
                type        TEXT NOT NULL DEFAULT 'bible',
                updatedate  TEXT NOT NULL,
                installdate TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS installed_files (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                module_name TEXT NOT NULL,
                file_name   TEXT NOT NULL,
                file_path   TEXT NOT NULL,
                FOREIGN KEY (module_name) REFERENCES installed_modules(name) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_installed_module ON installed_files(module_name);
        """)
        # Migration: silently add 'type' column to databases created before this schema
        try:
            conn.execute("ALTER TABLE installed_modules ADD COLUMN type TEXT NOT NULL DEFAULT 'bible'")
        except sqlite3.OperationalError:
            pass  # column already exists

def _inst_get(name):
    """Returns (mod_dict, [file_dicts]) or None."""
    with _inst_conn() as conn:
        row = conn.execute(
            "SELECT * FROM installed_modules WHERE LOWER(name) LIKE LOWER(?)", [name]
        ).fetchone()
        if row is None:
            return None
        files = conn.execute(
            "SELECT file_name, file_path FROM installed_files WHERE module_name = ?",
            [row["name"]],
        ).fetchall()
        return dict(row), [dict(f) for f in files]

def _inst_list(language=None, module_type=None):
    """Returns list of (mod_dict, [file_dicts])."""
    sql = "SELECT * FROM installed_modules WHERE 1=1"
    params = []
    if language:
        sql += " AND LOWER(language) LIKE LOWER(?)"
        params.append(f"%{language}%")
    if module_type:
        sql += " AND LOWER(type) LIKE LOWER(?)"
        params.append(f"%{module_type}%")
    sql += " ORDER BY LOWER(name)"
    with _inst_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            files = conn.execute(
                "SELECT file_name, file_path FROM installed_files WHERE module_name = ?",
                [row["name"]],
            ).fetchall()
            result.append((dict(row), [dict(f) for f in files]))
        return result

def _inst_insert(mod_row, files_dict):
    """
    mod_row: dict with keys name/language/description/type/updatedate/installdate
    files_dict: {file_name: file_path_str, ...}
    """
    with _inst_conn() as conn:
        conn.execute(
            "INSERT INTO installed_modules "
            "(name,language,description,type,updatedate,installdate) "
            "VALUES (:name,:language,:description,:type,:updatedate,:installdate)",
            mod_row,
        )
        conn.executemany(
            "INSERT INTO installed_files (module_name,file_name,file_path) VALUES (?,?,?)",
            [(mod_row["name"], fname, fpath) for fname, fpath in files_dict.items()],
        )

def _inst_delete(name):
    with _inst_conn() as conn:
        conn.execute("DELETE FROM installed_files WHERE module_name = ?", [name])
        conn.execute("DELETE FROM installed_modules WHERE name = ?", [name])

# ---------------------------------------------------------------------------
# CLI decorator
# ---------------------------------------------------------------------------

def use_installed_db(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not get_module_path():
            console.print("[red]Module path not set. Use 'set-path' first.[/red]")
            return
        ensure_installed_db()
        return f(*args, **kwargs)
    return decorated_function

# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------

def process_module_names(names_tuple):
    processed = []
    for name in names_tuple:
        parts = [part.strip().strip('\'"') for part in name.split(',')]
        processed.extend(p for p in parts if p)
    return processed

def ensure_dirs():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    REGISTRY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not any(SOURCES_DIR.iterdir()):
        init_sources()

def get_config():
    if not CONFIG_FILE_PATH.exists():
        return {}
    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}

def save_config(config):
    with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)

def get_etag_cache():
    if not ETAG_CACHE_PATH.exists():
        return {}
    try:
        with open(ETAG_CACHE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}

def save_etag_cache(etag_cache):
    with open(ETAG_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(etag_cache, f, indent=2)

def init_sources(force=False):
    for filename, url in DEFAULT_SOURCES.items():
        filepath = SOURCES_DIR / filename
        if not filepath.exists() or force:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(url)
    if force:
        console.print("[green]Default sources reinitialized.[/green]")

def get_module_path():
    return get_config().get("module_path")

# ---------------------------------------------------------------------------
# Registry parsers
# ---------------------------------------------------------------------------

def parse_extra_registry(data, registry_url):
    for mod in data.get("modules", []):
        if not all(k in mod for k in ['download_url', 'file_name', 'description', 'update_date']):
            continue
        module_name = mod["file_name"]
        if module_name.endswith('.zip'):
            module_name = module_name[:-4]
        download_url = mod["download_url"]
        url_filename = urllib.parse.unquote(download_url.split('/')[-1]).removesuffix('.zip')
        parts = url_filename.split('.')
        module_type = parts[-1] if len(parts) > 1 else 'bible'
        yield {
            "name": module_name, "language": mod.get("language_code"),
            "description": mod["description"], "update_date": mod["update_date"],
            "download_url": download_url, "file_name": mod["file_name"] + ".zip",
            "module_type": module_type, "size": None, "source_registry": registry_url,
        }

def parse_zipped_registry(data, registry_url):
    hosts = {h["alias"]: h["path"] for h in data.get("hosts", [])}
    for mod in data.get("downloads", []):
        if not (mod.get('abr') and mod.get('url')):
            continue
        for url_template in mod.get('url', []):
            alias_start = url_template.find("{") + 1
            alias_end = url_template.find("}")
            if alias_start == 0 or alias_end == -1:
                continue
            alias = url_template[alias_start:alias_end]
            file_part = url_template[alias_end + 1:]
            if alias not in hosts:
                continue
            name = mod.get('abr')
            file_name = mod.get('fil', name)
            if not (mod.get('des') and mod.get('upd')):
                continue
            download_url = hosts[alias].replace("%s", file_part)
            url_filename = urllib.parse.unquote(download_url.split('/')[-1]).removesuffix('.zip')
            parts = url_filename.split('.')
            module_type = parts[-1] if len(parts) > 1 else 'bible'
            yield {
                "name": name, "language": mod.get('lng'), "description": mod.get('des'),
                "update_date": mod.get('upd'), "download_url": download_url,
                "file_name": file_name, "module_type": module_type,
                "size": mod.get('siz'), "source_registry": registry_url,
            }

# ---------------------------------------------------------------------------
# Cache update
# ---------------------------------------------------------------------------

def update_cache():
    ensure_dirs()
    ensure_cache_db()
    etag_cache = get_etag_cache()

    with _cache_conn() as conn:
        conn.execute("DELETE FROM cached_modules")

    session = requests.Session()
    source_files = (
        list(SOURCES_DIR.glob("*.registry")) + list(SOURCES_DIR.glob("*.extra"))
    )

    with Progress(
        SpinnerColumn(), TextColumn("{task.description}"), BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%", TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[green]Processing sources...", total=len(source_files))
        for source_file in source_files:
            progress.update(task, advance=1, description=f"[cyan]Processing {source_file.name}")
            url = source_file.read_text(encoding='utf-8').strip()
            cached_reg_path = REGISTRY_CACHE_DIR / source_file.name
            headers = {'User-Agent': 'mybible-get/1.0'}
            if url in etag_cache and cached_reg_path.exists():
                headers['If-None-Match'] = etag_cache[url]
            try:
                response = session.get(url, timeout=20, headers=headers)
                if response.status_code == 304:
                    content = cached_reg_path.read_bytes()
                else:
                    response.raise_for_status()
                    content = response.content
                    cached_reg_path.write_bytes(content)
                    if 'ETag' in response.headers:
                        etag_cache[url] = response.headers['ETag']
                if source_file.suffix == ".registry":
                    zf = zipfile.ZipFile(io.BytesIO(content))
                    json_name = next(n for n in zf.namelist() if n.endswith('.json'))
                    data = json.loads(zf.read(json_name))
                    modules_to_add = list(parse_zipped_registry(data, url))
                else:
                    data = json.loads(content)
                    modules_to_add = list(parse_extra_registry(data, url))
                if modules_to_add:
                    _cache_insert_many(modules_to_add)
            except Exception as e:
                console.log(f"[yellow]Failed to process {url}: {type(e).__name__} - {e}[/yellow]")

    save_etag_cache(etag_cache)
    console.print("[bold green]Cache update complete.[/bold green]")

# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_clean_list(results, title, page=False, is_upgradable=False):
    if not results:
        console.print("[yellow]No modules found for the given criteria.[/yellow]")
        return

    def generate_output():
        yield f"[bold green]{title}[/bold green]\n"
        for i, item in enumerate(results):
            text = Text()
            if is_upgradable:
                text.append("Name: ", style="bold magenta"); text.append(item["name"], style="cyan")
                text.append("\nInstalled: ", style="bold magenta"); text.append(item["updatedate"], style="red")
                text.append("\nAvailable: ", style="bold magenta"); text.append(item["latest_date"], style="green")
                text.append("\nDescription: ", style="bold magenta"); text.append(item["description"])
            else:
                text.append("Name: ", style="bold magenta"); text.append(item["name"], style="cyan")
                text.append("\nLanguage: ", style="bold magenta"); text.append((item.get("language") or 'N/A'), style="white")
                text.append("\nDescription: ", style="bold magenta"); text.append(item["description"])
                version_key = "update_date" if "update_date" in item else "updatedate"
                text.append("\nVersion: ", style="bold magenta"); text.append(item[version_key], style="green")
                if "files" in item:
                    text.append("\nFiles: ", style="bold magenta"); text.append(str(len(item["files"])), style="white")
            yield text
            if i < len(results) - 1:
                yield Rule(style="dim blue")
            else:
                yield ""

    if page:
        with console.pager(styles=True):
            console.print(*list(generate_output()))
    else:
        for item in generate_output():
            console.print(item)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def cli():
    """A command-line tool to manage MyBible modules."""
    ensure_dirs()

@cli.command("set-path", help="Set the path where modules will be installed.")
@click.argument("path", type=click.Path())
def set_path(path):
    p = Path(path).resolve()
    if not p.exists() and click.confirm(f"Path '{p}' does not exist. Create it?"):
        p.mkdir(parents=True, exist_ok=True)
    elif not p.exists():
        console.print("[red]Aborted.[/red]"); return
    save_config({"module_path": str(p)})
    console.print(f"[green]Module path set to: {p}[/green]")

@cli.command("update", help="Update the module cache from all sources.")
def update_command():
    update_cache()

@cli.command("list", help="List available, installed, or upgradable modules.")
@click.option("--available", "-a", is_flag=True, help="List all available modules.")
@click.option("--installed", "-i", is_flag=True, help="List all installed modules.")
@click.option("--upgradable", "-u", is_flag=True, help="List modules with available updates.")
@click.option("-l", "--lang", "--language", "language", help="Filter by language code.")
@click.option("-t", "--type", "module_type", help="Filter by module type (e.g., bible, dictionary).")
@click.option("-p", "--page", is_flag=True, help="Display output one page at a time.")
@use_installed_db
def list_modules(available, installed, upgradable, language, module_type, page):
    if not any([available, installed, upgradable]):
        available = True

    if available:
        if is_cache_empty():
            console.print("[yellow]Cache is empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]"); return
        rows = _cache_list(language, module_type)
        filters = []
        if language: filters.append(f"Language: {language}")
        if module_type: filters.append(f"Type: {module_type}")
        title = f"Available Modules ({', '.join(filters)})" if filters else "All Available Modules"
        render_clean_list(rows, title, page)

    if installed and not upgradable:
        pairs = _inst_list(language, module_type)
        results = []
        for mod_row, files in pairs:
            mod_row["files"] = files
            results.append(mod_row)
        filters = []
        if language: filters.append(f"Language: {language}")
        if module_type: filters.append(f"Type: {module_type}")
        title = f"Installed Modules ({', '.join(filters)})" if filters else "Installed Modules"
        render_clean_list(results, title, page)

    if upgradable:
        pairs = _inst_list(language)
        if not pairs:
            console.print("[yellow]No modules installed, nothing to upgrade.[/yellow]"); return
        if is_cache_empty():
            console.print("[yellow]Cache is empty. Run 'update' to check for upgrades.[/yellow]"); return
        upgradable_list = []
        for mod_row, _ in pairs:
            if module_type and mod_row.get("type", "bible").lower() != module_type.lower():
                continue
            latest = _cache_get_module(mod_row["name"])
            if not latest or latest["update_date"] <= mod_row["updatedate"]:
                continue
            upgradable_list.append({
                "name": mod_row["name"],
                "updatedate": mod_row["updatedate"],
                "latest_date": latest["update_date"],
                "description": mod_row["description"],
            })
        filters = []
        if language: filters.append(f"Language: {language}")
        if module_type: filters.append(f"Type: {module_type}")
        title = f"Upgradable Modules ({', '.join(filters)})" if filters else "Upgradable Modules"
        render_clean_list(upgradable_list, title, page, is_upgradable=True)

@cli.command("search", help="Search for modules in the cache.")
@click.option("-q", "--query", "search_term", help="General search term for all fields.")
@click.option("-n", "--name", "search_name", help="Search term for the module name.")
@click.option("-d", "--desc", "--description", "search_desc", help="Search term for the description.")
@click.option("-l", "--lang", "--language", "search_lang", help="Search term for the language code.")
@click.option("-t", "--type", "search_type", help="Search term for the module type.")
@click.option("-p", "--page", is_flag=True, help="Display output one page at a time.")
def search(search_term, search_name, search_desc, search_lang, search_type, page):
    """Search for modules in the cache. Specific field searches use AND logic."""
    if is_cache_empty():
        console.print("[yellow]Cache is empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]"); return

    field_conds = []
    if search_name:  field_conds.append(("name", search_name))
    if search_desc:  field_conds.append(("description", search_desc))
    if search_lang:  field_conds.append(("language", search_lang))
    if search_type:  field_conds.append(("module_type", search_type))

    if not field_conds and not search_term:
        console.print("[yellow]Please provide a search term using --query, or a specific field like --name.[/yellow]"); return

    where_parts = []
    params = []
    for col, val in field_conds:
        where_parts.append(f"LOWER({col}) LIKE LOWER(?)")
        params.append(f"%{val}%")
    if search_term:
        where_parts.append(
            "(LOWER(name) LIKE LOWER(?) OR LOWER(description) LIKE LOWER(?) "
            "OR LOWER(language) LIKE LOWER(?) OR LOWER(module_type) LIKE LOWER(?))"
        )
        params.extend([f"%{search_term}%"] * 4)

    sql = ("SELECT * FROM cached_modules WHERE " + " AND ".join(where_parts)
           + " GROUP BY LOWER(name) ORDER BY LOWER(name)")
    with _cache_conn() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    title = f"Search Results for '{search_term}'" if search_term and not field_conds else "Search Results"
    render_clean_list(rows, title, page)

@cli.command("info", help="Show detailed information about a specific module.")
@click.argument("name")
@use_installed_db
def info(name):
    if is_cache_empty():
        console.print("[yellow]Cache empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]"); return
    cached = _cache_get_module(name)
    if not cached:
        console.print(f"[red]Module '{name}' not found.[/red]"); return
    cached = dict(cached)

    content_str = ""
    for k, v in cached.items():
        content_str += f"[bold magenta]{k.replace('_', ' ').title()}:[/bold magenta] {escape(str(v))}\n"

    inst = _inst_get(name)
    if inst:
        mod_row, files = inst
        status = (
            "[green]Up-to-date[/green]"
            if mod_row["updatedate"] >= cached["update_date"]
            else f"[yellow]Update to {cached['update_date']} available[/yellow]"
        )
        content_str += f"\n[bold magenta]Installed:[/bold magenta] [cyan]{mod_row['updatedate']}[/cyan] ({status})"
        if files:
            content_str += "\n[bold magenta]Installed Files:[/bold magenta]"
            for f in files:
                content_str += f"\n  • {escape(f['file_name'])}"
    else:
        content_str += "\n[bold magenta]Installed:[/bold magenta] [red]No[/red]"

    console.print(Panel(content_str, title=f"[bold]Info for {cached['name']}[/bold]",
                        box=ROUNDED, border_style="blue", expand=False))

# ---------------------------------------------------------------------------
# Install / remove helpers
# ---------------------------------------------------------------------------

def _reconstruct_sqlite_name(original_filename, module_name_from_json):
    if not original_filename.lower().endswith('.sqlite3'):
        return original_filename
    module_types = [
        "commentaries", "cross-references", "crossreferences", "devotions",
        "dictionaries_lookup", "dictionaries-lookup", "dictionary",
        "plan", "referencedata", "subheadings",
    ]
    lower_fn = original_filename.lower()
    for mod_type in module_types:
        if f".{mod_type.lower()}." in lower_fn:
            return f"{module_name_from_json}.{mod_type}.SQLite3"
    return f"{module_name_from_json}.SQLite3"

def install_single_module(name, version=None):
    """Download, extract and record a single module. Returns True on success."""
    mod = _cache_get_module(name, version)
    if not mod:
        console.print(f"[red]Module '{name}' not found in cache.[/red]")
        return False
    mod = dict(mod)

    url_filename = urllib.parse.unquote(mod["download_url"].split('/')[-1])
    if not url_filename.endswith('.zip'):
        url_filename += '.zip'
    zip_path = DOWNLOAD_CACHE_DIR / url_filename

    if not zip_path.exists():
        try:
            with Progress(
                TextColumn("[blue]{task.fields[filename]}"),
                BarColumn(), "[progress.percentage]{task.percentage:>3.1f}%",
                console=console,
            ) as p:
                task = p.add_task("Downloading", total=None, filename=mod["file_name"])
                with requests.get(mod["download_url"], stream=True, timeout=30) as r:
                    r.raise_for_status()
                    p.update(task, total=int(r.headers.get('content-length', 0)))
                    with open(zip_path, 'wb') as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk); p.update(task, advance=len(chunk))
        except requests.RequestException as e:
            console.print(f"[red]Download failed for '{name}': {e}[/red]"); return False

    final_files = {}
    module_path = get_module_path()
    clean_module_name = mod["name"].removesuffix('.zip')

    with console.status(f"[bold green]Extracting {mod['file_name']}..."), \
         zipfile.ZipFile(zip_path) as z:
        for member in z.infolist():
            if member.is_dir():
                continue
            orig = member.filename
            target_name = _reconstruct_sqlite_name(orig, clean_module_name)
            if target_name == orig and orig.startswith('.'):
                target_name = clean_module_name + orig
            z.extract(member, module_path)
            extracted_path = Path(module_path) / orig
            target_path = Path(module_path) / target_name
            if target_name != orig and extracted_path.exists():
                extracted_path.rename(target_path)
            final_files[target_name] = str(target_path)

    _inst_insert(
        {
            "name": mod["name"],
            "language": mod["language"],
            "description": mod["description"],
            "type": mod["module_type"],
            "updatedate": mod["update_date"],
            "installdate": datetime.now(timezone.utc).isoformat(),
        },
        final_files,
    )
    console.print(
        f"[bold green]Successfully installed {mod['name']} v{mod['update_date']} "
        f"({len(final_files)} files)[/bold green]"
    )
    return True

def remove_module(name, quiet=False):
    """Remove a single installed module. Returns True on success."""
    inst = _inst_get(name)
    if not inst:
        if not quiet:
            console.print(f"[yellow]'{name}' is not installed.[/yellow]")
        return False
    mod_row, files = inst
    for f in files:
        fp = Path(f["file_path"])
        if fp.exists():
            try:
                fp.unlink() if fp.is_file() else shutil.rmtree(fp)
            except OSError as e:
                if not quiet:
                    console.print(f"[red]Error removing {fp}: {e}[/red]")
                return False
    _inst_delete(mod_row["name"])
    if not quiet:
        console.print(f"[green]Module '{name}' removed.[/green]")
    return True

# ---------------------------------------------------------------------------
# Install / remove / upgrade commands
# ---------------------------------------------------------------------------

@cli.command("install")
@click.argument("names", nargs=-1, required=True)
@click.option("--version", help="Specify a version (update_date) to install. Only works with one module.")
@click.option("--reinstall", is_flag=True, help="Remove existing module before installing.")
@use_installed_db
def install(names, version, reinstall):
    """Install one or more modules by name. Supports comma or space separation."""
    module_names = process_module_names(names)
    if version and len(module_names) > 1:
        console.print("[red]The --version option only works with a single module.[/red]"); return
    if is_cache_empty():
        console.print("[yellow]Cache empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]"); return

    for name in module_names:
        console.print(f"--> Processing '{name}'...")
        if _inst_get(name):
            if reinstall:
                console.print(f"[yellow]Reinstalling '{name}' (removing existing version first)...[/yellow]")
                if not remove_module(name, quiet=True):
                    console.print(f"[red]Failed to remove existing '{name}', skipping.[/red]"); continue
            else:
                console.print(
                    f"[yellow]Module '{name}' is already installed. "
                    "Use 'upgrade' to update, or '--reinstall' to reinstall.[/yellow]"
                ); continue
        install_single_module(name, version)

@cli.command("remove")
@click.argument("names", nargs=-1, required=True)
@use_installed_db
def remove_command(names):
    """Remove one or more installed modules."""
    for name in process_module_names(names):
        remove_module(name)

@cli.command("uninstall", help="Remove one or more installed modules (synonym for remove).")
@click.argument("names", nargs=-1, required=True)
@use_installed_db
def uninstall_command(names):
    """Remove one or more installed modules."""
    for name in process_module_names(names):
        remove_module(name)

@cli.command("upgrade")
@click.argument("names", nargs=-1)
@click.option("--all", "upgrade_all", is_flag=True, help="Upgrade all outdated modules.")
@use_installed_db
def upgrade(names, upgrade_all):
    """Upgrade installed modules to their latest versions."""
    if not names and not upgrade_all:
        console.print("[yellow]Specify module name(s) or use --all.[/yellow]"); return
    if is_cache_empty():
        console.print("[red]Cache is empty. Run 'update' first.[/red]"); return

    to_upgrade = []
    if upgrade_all:
        pairs = _inst_list()
        if not pairs:
            console.print("[yellow]No modules installed, nothing to upgrade.[/yellow]"); return
        for mod_row, _ in pairs:
            latest = _cache_get_module(mod_row["name"])
            if latest and latest["update_date"] > mod_row["updatedate"]:
                to_upgrade.append(mod_row["name"])
    else:
        for name in process_module_names(names):
            inst = _inst_get(name)
            if not inst:
                console.print(f"[yellow]'{name}' is not installed.[/yellow]"); continue
            mod_row, _ = inst
            latest = _cache_get_module(name)
            if not latest:
                console.print(f"[red]Module '{name}' not found in cache.[/red]"); continue
            if latest["update_date"] > mod_row["updatedate"]:
                to_upgrade.append(name)

    if not to_upgrade:
        console.print("[green]All specified modules are up-to-date.[/green]"); return
    console.print(f"Found {len(to_upgrade)} module(s) to upgrade.")
    for module_name in to_upgrade:
        console.print(f"--> Upgrading '{module_name}'...")
        if remove_module(module_name, quiet=True):
            install_single_module(module_name)
        else:
            console.print(f"[red]Failed to remove old version of '{module_name}', skipping.[/red]")

@cli.command("versions", help="Show all available versions of a module.")
@click.argument("name")
@use_installed_db
def versions(name):
    if is_cache_empty():
        console.print("[yellow]Cache empty. Run 'update' first.[/yellow]"); return
    rows = _cache_versions(name)
    if not rows:
        console.print(f"[red]Module '{name}' not found.[/red]"); return
    inst = _inst_get(name)
    installed_version = inst[0]["updatedate"] if inst else None
    content = Text()
    for row in rows:
        ver = row["update_date"]
        marker = " [bold green](Installed)[/bold green]" if ver == installed_version else ""
        content.append(f"{ver}{marker}\n")
    console.print(Panel(content, title=f"Available Versions for {name}", box=ROUNDED, border_style="blue"))

@cli.command("reinit", help="Reinitialize the default module sources.")
def reinit():
    """Restore the default set of sources list."""
    if click.confirm(
        f"This will overwrite your current sources list.\n"
        f"Extra registries ('{CONFIG_DIR}/sources/<filename>.extra') will remain intact.\n"
        "Are you sure?"
    ):
        init_sources(force=True)

@cli.command("purge", help="Clear cache or remove all configuration.")
@click.option("--full", is_flag=True, help="Remove the entire configuration directory.")
def purge(full):
    """Remove cached downloads, registries, and optionally the entire config."""
    if full:
        if click.confirm(
            f"This will permanently delete the entire configuration directory at {CONFIG_DIR}. Are you sure?"
        ):
            shutil.rmtree(CONFIG_DIR)
            console.print("[green]Entire configuration directory purged.[/green]")
    else:
        if click.confirm(
            f"This will delete all files in {CACHE_DIR} and {REGISTRY_CACHE_DIR}. Are you sure?"
        ):
            if CACHE_DIR.exists(): shutil.rmtree(CACHE_DIR)
            if REGISTRY_CACHE_DIR.exists(): shutil.rmtree(REGISTRY_CACHE_DIR)
            if ETAG_CACHE_PATH.exists(): ETAG_CACHE_PATH.unlink()
            console.print("[green]Cache purged.[/green]")

if __name__ == '__main__':
    cli()

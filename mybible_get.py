#!/usr/bin/env python3
import click
import requests
import peewee as pw
import json
import zipfile
import io
import os
import sys
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from functools import reduce, wraps
import operator
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.markup import escape
from rich.box import ROUNDED
from rich.rule import Rule
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from email.utils import parsedate_to_datetime

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
CACHE_DB_PATH = CONFIG_DIR / "cache.db"
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

# --- DATABASE MODELS ---
cache_db = pw.SqliteDatabase(CACHE_DB_PATH)
installed_db = pw.SqliteDatabase(None)  # Deferred initialization

class BaseModel(pw.Model):
    class Meta:
        database = cache_db

class CachedModule(BaseModel):
    name = pw.TextField()
    language = pw.TextField(null=True)
    description = pw.TextField()
    update_date = pw.TextField()
    download_url = pw.TextField()
    file_name = pw.TextField()
    module_type = pw.TextField()
    size = pw.TextField(null=True)
    source_registry = pw.TextField()

    class Meta:
        primary_key = pw.CompositeKey('name', 'update_date', 'download_url')

class InstalledBaseModel(pw.Model):
    class Meta:
        database = installed_db

class InstalledModule(InstalledBaseModel):
    name = pw.TextField(unique=True)
    language = pw.TextField(null=True)
    description = pw.TextField()
    update_date = pw.TextField()
    install_date = pw.DateTimeField()

class InstalledFile(InstalledBaseModel):
    module = pw.ForeignKeyField(InstalledModule, backref='files', on_delete='CASCADE')
    file_name = pw.TextField()
    file_path = pw.TextField()

# --- HELPER FUNCTIONS & DECORATORS ---
def use_installed_db(f):
    """Decorator to manage installed database connection lifecycle."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        module_path = get_module_path()
        if not module_path:
            console.print("[red]Module path not set. Use 'set-path' first.[/red]")
            return
        
        db_path = Path(module_path) / "mybible_installed.db"
        try:
            installed_db.init(str(db_path))
            installed_db.connect(reuse_if_open=True)
            installed_db.create_tables([InstalledModule, InstalledFile], safe=True)
            return f(*args, **kwargs)
        finally:
            if not installed_db.is_closed():
                installed_db.close()
    return decorated_function

def process_module_names(names_tuple):
    """Cleans up module names from CLI input, handling commas and quotes."""
    processed = []
    for name in names_tuple:
        # Split by comma and strip whitespace/quotes from each part
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
    if not CONFIG_FILE_PATH.exists(): return {}
    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f: return json.load(f)
    except json.JSONDecodeError: return {}

def save_config(config):
    with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f: json.dump(config, f, indent=2)

def get_etag_cache():
    if not ETAG_CACHE_PATH.exists(): return {}
    try:
        with open(ETAG_CACHE_PATH, 'r', encoding='utf-8') as f: return json.load(f)
    except json.JSONDecodeError: return {}

def save_etag_cache(etag_cache):
    with open(ETAG_CACHE_PATH, 'w', encoding='utf-8') as f: json.dump(etag_cache, f, indent=2)

def init_sources(force=False):
    for filename, url in DEFAULT_SOURCES.items():
        filepath = SOURCES_DIR / filename
        if not filepath.exists() or force:
            with open(filepath, 'w', encoding='utf-8') as f: f.write(url)
    if force: console.print("[green]Default sources reinitialized.[/green]")

def get_module_path(): return get_config().get("module_path")

def ensure_cache_db():
    """Ensures cache database exists and tables are created."""
    cache_db.connect(reuse_if_open=True)
    cache_db.create_tables([CachedModule], safe=True)

def is_cache_empty():
    """
    Safely checks if the cache is empty.
    Ensures database and tables exist before checking.
    """
    if not CACHE_DB_PATH.exists():
        return True
    try:
        ensure_cache_db()
        return not CachedModule.select().exists()
    except pw.OperationalError:
        return True
    finally:
        if not cache_db.is_closed():
            cache_db.close()

# --- CORE LOGIC ---
def parse_extra_registry(data, registry_url):
    for mod in data.get("modules", []):
        if not all(k in mod for k in ['download_url', 'file_name', 'description', 'update_date']): continue
        
        # Remove .zip extension from file_name when using it as name
        module_name = mod["file_name"]
        if module_name.endswith('.zip'):
            module_name = module_name[:-4]

        # Extract module type from download URL
        download_url = mod["download_url"]
        url_filename = download_url.split('/')[-1]
        decoded_url_filename = urllib.parse.unquote(url_filename)
        url_without_zip = decoded_url_filename.removesuffix('.zip')
        parts = url_without_zip.split('.')
        module_type = parts[-1] if len(parts) > 1 else 'bible'

        yield {
            "name": module_name, "language": mod.get("language_code"), "description": mod["description"],
            "update_date": mod["update_date"], "download_url": download_url,
            "file_name": mod["file_name"] + ".zip", "module_type": module_type, "size": None,
            "source_registry": registry_url,
        }

def parse_zipped_registry(data, registry_url):
    hosts = {h["alias"]: h["path"] for h in data.get("hosts", [])}
    for mod in data.get("downloads", []):
        if not (mod.get('abr') and mod.get('url')): continue
        for url_template in mod.get('url', []):
            alias_start, alias_end = url_template.find("{") + 1, url_template.find("}")
            if alias_start == 0 or alias_end == -1: continue
            alias, file_part = url_template[alias_start:alias_end], url_template[alias_end + 1:]
            if alias in hosts:
                name, file_name = mod.get('abr'), mod.get('fil', mod.get('abr'))
                if not (mod.get('des') and mod.get('upd')): continue
                
                # Build download URL and extract module type from it
                download_url = hosts[alias].replace("%s", file_part)
                url_filename = download_url.split('/')[-1]
                decoded_url_filename = urllib.parse.unquote(url_filename)
                url_without_zip = decoded_url_filename.removesuffix('.zip')
                parts = url_without_zip.split('.')
                module_type = parts[-1] if len(parts) > 1 else 'bible'
                
                yield {
                    "name": name, "language": mod.get('lng'), "description": mod.get('des'),
                    "update_date": mod.get('upd'), "download_url": download_url,
                    "file_name": file_name, "module_type": module_type,
                    "size": mod.get('siz'), "source_registry": registry_url,
                }

def update_cache():
    ensure_dirs()
    ensure_cache_db()
    etag_cache = get_etag_cache()
    
    with console.status("[bold green]Updating module cache..."):
        CachedModule.delete().execute()
        session, source_files = requests.Session(), list(SOURCES_DIR.glob("*.registry")) + list(SOURCES_DIR.glob("*.extra"))
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeElapsedColumn(), console=console) as progress:
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
                    
                    data = json.loads(zipfile.ZipFile(io.BytesIO(content)).read(next(n for n in zipfile.ZipFile(io.BytesIO(content)).namelist() if n.endswith('.json')))) if source_file.suffix == ".registry" else json.loads(content)
                    parser = parse_zipped_registry if source_file.suffix == ".registry" else parse_extra_registry
                    modules_to_add = list(parser(data, url))
                    if modules_to_add:
                        with cache_db.atomic(): CachedModule.insert_many(modules_to_add).on_conflict_ignore().execute()
                        
                except (requests.RequestException, json.JSONDecodeError, zipfile.BadZipFile, KeyError, AttributeError, StopIteration) as e:
                    console.log(f"[yellow]Failed to process {url}: {type(e).__name__} - {e}[/yellow]")
    
    save_etag_cache(etag_cache)
    console.print("[bold green]Cache update complete.[/bold green]")

# --- CLI COMMANDS ---
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def cli():
    """A command-line tool to manage MyBible modules."""
    ensure_dirs()

@cli.command("set-path", help="Set the path where modules will be installed.")
@click.argument("path", type=click.Path())
def set_path(path):
    p = Path(path).resolve()
    if not p.exists() and click.confirm(f"Path '{p}' does not exist. Create it?"): p.mkdir(parents=True, exist_ok=True)
    elif not p.exists(): console.print("[red]Aborted.[/red]"); return
    save_config({"module_path": str(p)}); console.print(f"[green]Module path set to: {p}[/green]")

@cli.command("update", help="Update the module cache from all sources.")
def update_command(): update_cache()

def render_clean_list(query_results, title, page=False, is_upgradable=False):
    results = list(query_results)
    if not results:
        console.print(f"[yellow]No modules found for the given criteria.[/yellow]"); return

    def generate_output():
        yield f"[bold green]{title}[/bold green]\n"
        for i, mod in enumerate(results):
            text = Text()
            if is_upgradable:
                text.append("Name: ", style="bold magenta"); text.append(mod.name, style="cyan")
                text.append("\nInstalled: ", style="bold magenta"); text.append(mod.update_date, style="red")
                text.append("\nAvailable: ", style="bold magenta"); text.append(mod.latest_date, style="green")
                text.append("\nDescription: ", style="bold magenta"); text.append(mod.description)
            else:
                text.append("Name: ", style="bold magenta"); text.append(mod.name, style="cyan")
                text.append("\nLanguage: ", style="bold magenta"); text.append((mod.language or 'N/A'), style="white")
                text.append("\nDescription: ", style="bold magenta"); text.append(mod.description)
                text.append("\nVersion: ", style="bold magenta"); text.append(mod.update_date, style="green")
                if hasattr(mod, 'files') and hasattr(mod.files, '__len__'):
                    file_count = len([f for f in mod.files])
                    text.append(f"\nFiles: ", style="bold magenta"); text.append(f"{file_count}", style="white")
            yield text
            if i < len(results) - 1: yield Rule(style="dim blue")
            else: yield ""

    if page:
        with console.pager(styles=True): console.print(*list(generate_output()))
    else:
        for item in generate_output(): console.print(item)

@cli.command("list", help="List available, installed, or upgradable modules.")
@click.option("--available", "-a", is_flag=True, help="List all available modules.")
@click.option("--installed", "-i", is_flag=True, help="List all installed modules.")
@click.option("--upgradable", "-u", is_flag=True, help="List modules with available updates.")
@click.option("-l", "--lang", "--language", "language", help="Filter by language code.")
@click.option("-t", "--type", "module_type", help="Filter by module type (e.g., bible, dictionary).")
@click.option("-p", "--page", is_flag=True, help="Display output one page at a time.")
@use_installed_db
def list_modules(available, installed, upgradable, language, module_type, page):
    if not any([available, installed, upgradable]): available = True
    
    if available:
        if is_cache_empty(): console.print("[yellow]Cache is empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]"); return
        ensure_cache_db()
        query = CachedModule.select().order_by(pw.fn.LOWER(CachedModule.name)).group_by(pw.fn.LOWER(CachedModule.name))
        title = "All Available Modules"
        if language: 
            query = query.where(CachedModule.language.ilike(language))
        if module_type:
            query = query.where(CachedModule.module_type.ilike(module_type))
        
        # Update title to reflect filters
        filters = []
        if language: filters.append(f"Language: {language}")
        if module_type: filters.append(f"Type: {module_type}")
        if filters: title = f"Available Modules ({', '.join(filters)})"
        
        render_clean_list(query, title, page)

    if installed and not upgradable:
        query = InstalledModule.select().order_by(pw.fn.LOWER(InstalledModule.name))
        title = "Installed Modules"
        if language:
            query = query.where(InstalledModule.language.ilike(language))

        # Since InstalledModule doesn't have module_type, we filter post-query if needed.
        results = list(query)
        if module_type:
            filtered_results = []
            ensure_cache_db()
            for inst_mod in results:
                # Find a corresponding cached module to check its type
                cached_mod = CachedModule.select().where(CachedModule.name.ilike(inst_mod.name)).get_or_none()
                if cached_mod and cached_mod.module_type.lower() == module_type.lower():
                    filtered_results.append(inst_mod)
            results = filtered_results

        # Update title to reflect filters
        filters = []
        if language: filters.append(f"Language: {language}")
        if module_type: filters.append(f"Type: {module_type}")
        if filters: title = f"Installed Modules ({', '.join(filters)})"

        render_clean_list(results, title, page)
        
    if upgradable:
        installed_modules = list(InstalledModule.select())
        if not installed_modules: console.print("[yellow]No modules installed, nothing to upgrade.[/yellow]"); return
        if is_cache_empty(): console.print("[yellow]Cache is empty. Run 'update' to check for upgrades.[/yellow]"); return
        ensure_cache_db()
        
        upgradable_modules = []
        for inst in installed_modules:
            # Apply language filter first
            if language and inst.language and language.lower() not in inst.language.lower():
                continue
                
            # Build the query for the latest cached version
            latest_cached = (CachedModule.select().where(CachedModule.name.ilike(inst.name)).order_by(CachedModule.update_date.desc()).get_or_none())
            
            if not latest_cached or latest_cached.update_date <= inst.update_date:
                continue
                
            # Apply type filter if provided
            if module_type and latest_cached.module_type.lower() != module_type.lower():
                continue

            class UpgradableMod:
                def __init__(self, i, c): self.name, self.update_date, self.latest_date, self.description = i.name, i.update_date, c.update_date, i.description
            upgradable_modules.append(UpgradableMod(inst, latest_cached))
        
        # Update title to reflect filters
        filters = []
        if language: filters.append(f"Language: {language}")
        if module_type: filters.append(f"Type: {module_type}")
        title = "Upgradable Modules"
        if filters: title = f"Upgradable Modules ({', '.join(filters)})"
        
        render_clean_list(upgradable_modules, title, page, is_upgradable=True)

@cli.command("search", help="Search for modules in the cache.")
@click.option("-q", "--query", "search_term", help="General search term for all fields.")
@click.option("-n", "--name", "search_name", help="Search term for the module name.")
@click.option("-d", "--desc", "--description", "search_desc", help="Search term for the description.")
@click.option("-l", "--lang", "--language", "search_lang", help="Search term for the language code.")
@click.option("-t", "--type", "search_type", help="Search term for the module type.")
@click.option("-p", "--page", is_flag=True, help="Display output one page at a time.")
def search(search_term, search_name, search_desc, search_lang, search_type, page):
    """Search for modules in the cache. Specific field searches are combined with AND logic."""
    if is_cache_empty():
        console.print("[yellow]Cache is empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]")
        return
    ensure_cache_db()

    conditions = []
    title = "Search Results"

    if search_name:
        conditions.append(CachedModule.name.ilike(f"%{search_name}%"))
    if search_desc:
        conditions.append(CachedModule.description.ilike(f"%{search_desc}%"))
    if search_lang:
        conditions.append(CachedModule.language.ilike(f"%{search_lang}%"))
    if search_type:
        conditions.append(CachedModule.module_type.ilike(f"%{search_type}%"))

    # If user provided specific field searches, use AND logic.
    if conditions:
        where_clause = reduce(operator.and_, conditions)
        # If there's also a general query, combine it with the specific ones using AND
        if search_term:
            term = f"%{search_term}%"
            general_clause = (
                CachedModule.name.ilike(term) |
                CachedModule.description.ilike(term)
            )
            where_clause = where_clause & general_clause

    # If not, but a general term is provided, use OR logic across fields.
    elif search_term:
        term = f"%{search_term}%"
        where_clause = (
            CachedModule.name.ilike(term) |
            CachedModule.description.ilike(term) |
            CachedModule.language.ilike(term) |
            CachedModule.module_type.ilike(term)
        )
        title = f"Search Results for '{search_term}'"
    # If no terms are provided at all.
    else:
        console.print("[yellow]Please provide a search term using --query, or a specific field like --name.[/yellow]")
        return

    query = (CachedModule.select()
             .where(where_clause)
             .group_by(pw.fn.LOWER(CachedModule.name))
             .order_by(pw.fn.LOWER(CachedModule.name)))
    render_clean_list(query, title, page)

@cli.command("info", help="Show detailed information about a specific module.")
@click.argument("name")
@use_installed_db
def info(name):
    if is_cache_empty(): 
        console.print("[yellow]Cache empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]")
        return
    ensure_cache_db()
    mods = list(CachedModule.select().where(CachedModule.name.ilike(name)).order_by(CachedModule.update_date.desc()))
    if not mods: console.print(f"[red]Module '{name}' not found.[/red]"); return
    latest = mods[0]
    
    content_str = ""
    for k,v in vars(latest)['__data__'].items():
        content_str += f"[bold magenta]{k.replace('_', ' ').title()}:[/bold magenta] {escape(str(v))}\n"

    if (installed := InstalledModule.get_or_none(InstalledModule.name.ilike(name))):
        status = "[green]Up-to-date[/green]" if installed.update_date >= latest.update_date else f"[yellow]Update to {latest.update_date} available[/yellow]"
        content_str += f"\n[bold magenta]Installed:[/bold magenta] [cyan]{installed.update_date}[/cyan] ({status})"
        if installed.files:
            content_str += f"\n[bold magenta]Installed Files:[/bold magenta]"
            for file_obj in installed.files: content_str += f"\n  • {escape(file_obj.file_name)}"
    else: 
        content_str += f"\n[bold magenta]Installed:[/bold magenta] [red]No[/red]"
    
    console.print(Panel(content_str, title=f"[bold]Info for {latest.name}[/bold]", box=ROUNDED, border_style="blue", expand=False))

def _reconstruct_sqlite_name(original_filename, module_name_from_json):
    """
    Reconstructs SQLite3 filenames to be consistent, using the clean module name
    from the JSON registry. This handles malformed filenames from zip archives.
    """
    if not original_filename.lower().endswith('.sqlite3'):
        return original_filename

    module_types = [
        "commentaries", "cross-references", "crossreferences", "devotions",
        "dictionaries_lookup", "dictionaries-lookup", "dictionary",
        "plan", "referencedata", "subheadings"
    ]
    
    lower_filename = original_filename.lower()
    detected_type = None
    for mod_type in module_types:
        if f".{mod_type.lower()}." in lower_filename:
            detected_type = mod_type
            break
            
    if detected_type:
        return f"{module_name_from_json}.{detected_type}.SQLite3"
    else:
        return f"{module_name_from_json}.SQLite3"

def install_single_module(name, version=None):
    """Core logic to install or upgrade a single module. Returns True on success."""
    query = CachedModule.select().where(CachedModule.name.ilike(name))
    mod = query.where(CachedModule.update_date == version).get_or_none() if version else query.order_by(CachedModule.update_date.desc()).get_or_none()
    if not mod: 
        console.print(f"[red]Module '{name}' not found in cache.[/red]")
        return False
    
    # URL decode the filename to handle UTF-8 characters properly
    url_filename = mod.download_url.split('/')[-1]
    decoded_filename = urllib.parse.unquote(url_filename)
    if not decoded_filename.endswith('.zip'):
        decoded_filename += '.zip'
    zip_path = DOWNLOAD_CACHE_DIR / decoded_filename
    
    if not zip_path.exists():
        try:
            with Progress(TextColumn("[blue]{task.fields[filename]}"), BarColumn(), "[progress.percentage]{task.percentage:>3.1f}%", console=console) as p:
                task = p.add_task("Downloading", total=None, filename=mod.file_name)
                with requests.get(mod.download_url, stream=True, timeout=30) as r:
                    r.raise_for_status(); p.update(task, total=int(r.headers.get('content-length', 0)))
                    with open(zip_path, 'wb') as f:
                        for chunk in r.iter_content(8192): f.write(chunk); p.update(task, advance=len(chunk))
        except requests.RequestException as e: 
            console.print(f"[red]Download failed for '{name}': {e}[/red]"); return False

    final_files_to_db = {}
    with console.status(f"[bold green]Extracting {mod.file_name}..."), zipfile.ZipFile(zip_path) as z:
        module_path = get_module_path()
        clean_module_name = mod.name.removesuffix('.zip')
        
        for member in z.infolist():
            # Skip directories
            if member.is_dir():
                continue

            original_filename_in_zip = member.filename
            
            # Reconstruct the name for SQLite3 files to ensure consistency
            target_name = _reconstruct_sqlite_name(original_filename_in_zip, clean_module_name)

            # If the name was not reconstructed (i.e., not a SQLite3 file),
            # handle the dot-prefix case.
            if target_name == original_filename_in_zip and original_filename_in_zip.startswith('.'):
                target_name = clean_module_name + original_filename_in_zip

            # Extract the file with its original name first
            z.extract(member, module_path)
            
            # If the target name is different, rename the extracted file
            extracted_path = Path(module_path) / original_filename_in_zip
            if target_name != original_filename_in_zip:
                target_path = Path(module_path) / target_name
                if extracted_path.exists():
                    extracted_path.rename(target_path)
                final_files_to_db[target_name] = target_path
            else:
                final_files_to_db[target_name] = extracted_path
    
    with installed_db.atomic():
        installed_module = InstalledModule.create(name=mod.name, language=mod.language, description=mod.description, update_date=mod.update_date, install_date=datetime.now())
        for fname, fpath in final_files_to_db.items():
            InstalledFile.create(module=installed_module, file_name=fname, file_path=str(fpath))

    console.print(f"[bold green]Successfully installed {mod.name} v{mod.update_date} ({len(final_files_to_db)} files)[/bold green]")
    return True

@cli.command("install")
@click.argument("names", nargs=-1, required=True)
@click.option("--version", help="Specify a version (update_date) to install. Only works with one module.")
@click.option("--reinstall", is_flag=True, help="Remove existing module before installing (reinstall fresh).")
@use_installed_db
def install(names, version, reinstall):
    """Install one or more modules by name. Supports comma or space separation."""
    if version and len(process_module_names(names)) > 1:
        console.print("[red]The --version option only works with a single module.[/red]"); return
    if is_cache_empty(): console.print("[yellow]Cache empty. Run '[bold cyan]update[/bold cyan]' first.[/yellow]"); return
    ensure_cache_db()
    
    module_names = process_module_names(names)
    for name in module_names:
        console.print(f"--> Processing '{name}'...")
        with installed_db.atomic():
            if (installed := InstalledModule.get_or_none(InstalledModule.name.ilike(name))):
                if reinstall:
                    console.print(f"[yellow]Reinstalling '{name}' (removing existing version first)...[/yellow]")
                    if not remove_module(name, quiet=True):
                        console.print(f"[red]Failed to remove existing '{name}', skipping.[/red]")
                        continue
                else:
                    console.print(f"[yellow]Module '{name}' is already installed. Use 'upgrade' to get a new version, or '--reinstall' to reinstall.[/yellow]")
                    continue
        install_single_module(name, version)

def remove_module(name, quiet=False):
    """Core logic to remove a single module. Returns True on success."""
    with installed_db.atomic():
        installed = InstalledModule.get_or_none(InstalledModule.name.ilike(name))
        if not installed:
            if not quiet: console.print(f"[yellow]'{name}' is not installed.[/yellow]")
            return False
        
        for file_record in installed.files:
            file_path = Path(file_record.file_path)
            if file_path.exists():
                try:
                    if file_path.is_file(): file_path.unlink()
                    elif file_path.is_dir(): shutil.rmtree(file_path)
                except OSError as e:
                    if not quiet: console.print(f"[red]Error removing {file_path}: {e}[/red]")
                    return False
        installed.delete_instance(recursive=True)
    if not quiet: console.print(f"[green]Module '{name}' removed.[/green]")
    return True

@cli.command("remove")
@click.argument("names", nargs=-1, required=True)
@use_installed_db
def remove_command(names):
    """Remove one or more installed modules."""
    module_names = process_module_names(names)
    for name in module_names:
        remove_module(name)

@cli.command("uninstall", help="Remove one or more installed modules (synonym for remove).")
@click.argument("names", nargs=-1, required=True)
@use_installed_db
def uninstall_command(names):
    """Remove one or more installed modules."""
    module_names = process_module_names(names)
    for name in module_names:
        remove_module(name)

@cli.command("upgrade")
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Upgrade all outdated modules.")
@use_installed_db
def upgrade(names, all):
    """Upgrade installed modules to their latest versions."""
    if not names and not all: console.print("[yellow]Specify module name(s) or use --all.[/yellow]"); return
    if is_cache_empty(): console.print("[red]Cache is empty. Run 'update' first.[/red]"); return
    ensure_cache_db()
    
    to_upgrade = []
    if all:
        installed_modules = list(InstalledModule.select())
        if not installed_modules: console.print("[yellow]No modules installed, nothing to upgrade.[/yellow]"); return
        for installed_mod in installed_modules:
            latest = (CachedModule.select().where(CachedModule.name.ilike(installed_mod.name)).order_by(CachedModule.update_date.desc()).get_or_none())
            if latest and latest.update_date > installed_mod.update_date:
                to_upgrade.append(installed_mod.name)
    else:
        module_names = process_module_names(names)
        for name in module_names:
            installed_mod = InstalledModule.get_or_none(InstalledModule.name.ilike(name))
            if not installed_mod: console.print(f"[yellow]'{name}' is not installed.[/yellow]"); continue
            latest = (CachedModule.select().where(CachedModule.name.ilike(name)).order_by(CachedModule.update_date.desc()).get_or_none())
            if not latest: console.print(f"[red]Module '{name}' not found in cache.[/red]"); continue
            if latest.update_date > installed_mod.update_date: to_upgrade.append(name)

    if not to_upgrade: console.print("[green]All specified modules are up-to-date.[/green]"); return
    console.print(f"Found {len(to_upgrade)} modules to upgrade.")
    for module_name in to_upgrade:
        console.print(f"--> Upgrading '{module_name}'...")
        if remove_module(module_name, quiet=True):
            install_single_module(module_name)
        else:
            console.print(f"[red]Failed to remove old version of '{module_name}', skipping upgrade.[/red]")

@cli.command("versions", help="Show all available versions of a module.")
@click.argument("name")
@use_installed_db
def versions(name):
    if is_cache_empty(): 
        console.print("[yellow]Cache empty. Run 'update' first.[/yellow]")
        return
    ensure_cache_db()
    mods = list(CachedModule.select().where(CachedModule.name.ilike(name)).order_by(CachedModule.update_date.desc()).group_by(CachedModule.update_date))
    if not mods: 
        console.print(f"[red]Module '{name}' not found.[/red]")
        return
    content = Text()
    installed_version = None
    installed = InstalledModule.get_or_none(InstalledModule.name.ilike(name))
    if installed:
        installed_version = installed.update_date
    
    for mod in mods:
        status = "[bold green](Installed)[/bold green]" if installed_version == mod.update_date else ""
        content.append(f"{mod.update_date} {status}\n")
    console.print(Panel(content, title=f"Available Versions for {name}", box=ROUNDED, border_style="blue"))

@cli.command("reinit", help="Reinitialize the default module sources.")
def reinit():
    """Restore the default set of sources list."""
    if click.confirm(f"This will overwrite your current sources list.\nExtra registries ('{CONFIG_DIR}/sources/<filename>.extra') will remain intact.\nAre you sure?"):
        init_sources(force=True)

@cli.command("purge", help="Clear cache or remove all configuration.")
@click.option("--full", is_flag=True, help="Remove the entire configuration directory.")
def purge(full):
    """Remove cached downloads, registries, and optionally the entire config."""
    if full:
        if click.confirm(f"This will permanently delete the entire configuration directory at {CONFIG_DIR}. Are you sure?"):
            shutil.rmtree(CONFIG_DIR)
            console.print("[green]Entire configuration directory purged.[/green]")
    else:
        if click.confirm(f"This will delete all files in {CACHE_DIR} and {REGISTRY_CACHE_DIR}. Are you sure?"):
            if CACHE_DIR.exists(): shutil.rmtree(CACHE_DIR)
            if REGISTRY_CACHE_DIR.exists(): shutil.rmtree(REGISTRY_CACHE_DIR)
            if ETAG_CACHE_PATH.exists(): ETAG_CACHE_PATH.unlink()
            console.print("[green]Cache purged.[/green]")

if __name__ == '__main__':
    cli()

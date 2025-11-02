# `mybible-get`

A command-line tool for discovering, downloading, and managing [MyBible.zone](https://mybible.zone) modules. This tool provides an efficient way to search and install available Bible translations, commentaries, dictionaries, and other biblical resources from multiple registry sources.

## Purpose and Motivation

`mybible-get` is designed as an auxiliary utility for managing MyBible modules. It is not intended as a Bible study tool. Its only purpose is to manage a local collection of MyBible modules in a way similar to package managers for various operating systems (`apt`, `brew`, `choco`, etc.).

## Features

- **Multi-source registry support** - Fetches modules from multiple MyBible registries
- **Intelligent caching** - Uses ETags and local caching to minimize network requests
- **Advanced search and filtering** - Search by name, description, language, or module type
- **Version management** - View available versions and upgrade installed modules
- **Cross-platform compatibility** - Works on Windows, macOS, and Linux
- **Rich console output** - Clean, colorized output using Rich library
- **Batch operations** - Install, remove, or upgrade multiple modules at once

## Installation

### Option 1: Install with pipx from this repository (Recommended)

```
pipx install git+https://github.com/kosivantsov/mybible-get.git
```

For a specific version or branch:
```
pipx install git+https://github.com/kosivantsov/mybible-get.git@v1.0.0
pipx install git+https://github.com/kosivantsov/mybible-get.git@main
```

### Option 2: Local installation with pipx

1. Clone or download this repository:
   ```
   git clone https://github.com/kosivantsov/mybible-get.git
   ```
2. Enter the local copy:
   ```
   cd mybible-get
   ```
2. Ensure you have both `mybible_get.py` and `pyproject.toml` in the same directory
3. Install locally:
   ```
   pipx install .
   ```

### Option 3: Direct execution

Simply run the script directly with Python 3.9+ (after installing dependencies manually):

```
python3 mybible_get.py --help
```

## Prerequisites

- Python 3.9 or higher
- Dependencies (automatically installed with pipx):
  - click
  - requests
  - peewee
  - rich

## Quick Start

1. **Set module installation path:**
   ```
   mybible-get set-path /path/to/your/mybible/modules
   ```
    You can manage more than one collection of modules with `mybible-get`. Keep in mind that it's best not to mix modules managed by this tool with those managed manually in the same folder.

2. **Update module cache:**
   ```
   mybible-get update
   ```

3. **List available modules:**
   ```
   mybible-get list --available
   ```

4. **Search for specific modules:**
   ```
   mybible-get search -l en -t bible
   mybible-get search -q "king james"
   ```

5. **Install modules:**
   ```
   mybible-get install "KJV" "ESV"
   ```

## Commands

| Command | Description |
|---------|-------------|
| `set-path` | Set the path where modules will be installed |
| `update` | Update the module cache from all sources |
| `list` | List available, installed, or upgradable modules |
| `search` | Search for modules in the cache |
| `info` | Show detailed information about a specific module |
| `install` | Install one or more modules by name |
| `remove` / `uninstall` | Remove one or more installed modules |
| `upgrade` | Upgrade installed modules to their latest versions |
| `versions` | Show all available versions of a module |
| `reinit` | Reinitialize the default module sources |
| `purge` | Clear cache or remove all configuration |

## Usage Examples

### Listing and Filtering

```
# List all available modules
mybible-get list --available

# List installed modules
mybible-get list --installed

# List upgradable modules
mybible-get list --upgradable

# Filter by language and type
mybible-get list --available -l uk -t commentaries
```

### Searching

```
# General search across all fields
mybible-get search -q "orthodox"

# Specific field searches (combined with AND logic)
mybible-get search -l cs -t bible -d "kralicka"

# Search by module name
mybible-get search -n "ESV"
```

### Module Management

```
# Get detailed information about a module
mybible-get info "RST"

# Install single module
mybible-get install "KJV"

# Install multiple modules
mybible-get install "ESV" "NIV" "NASB"

# Install with comma separation
mybible-get install "ESV", "NIV", "NASB"

# Install specific version
mybible-get install "KJV" --version "2020-01-15"

# Upgrade all outdated modules
mybible-get upgrade --all

# Upgrade specific modules
mybible-get upgrade "ESV" "NIV"

# Remove modules
mybible-get remove "OldModule"
mybible-get uninstall "AnotherModule"  # synonym for remove
```

### Advanced Operations

```
# View all available versions of a module
mybible-get versions "ESV"

# Reinstall a module (fresh installation)
mybible-get install "KJV" --reinstall

# Clear download cache
mybible-get purge

# Remove all configuration and cache
mybible-get purge --full

# Reinitialize default sources
mybible-get reinit
```

## Module Sources

The tool fetches modules from multiple default registries:

- **mybible.zone** - Primary official registry
- **myb.1gb.ru** - Community registry
- **mybible.infoo.pro** - Alternative registry
- **mph4.ru** - Additional community source
- **Dropbox** - Backup registry
- **Test registries** - Development sources

Additional sources can be added by placing `.registry` or `.extra` files in the sources directory.
Both `.registry` and `.extra` files are plain text files containing only one URL to either a 'core' registry (`registry.zip`), or an extra registry supported in later versions of the MyBible app for Android.


## Configuration

Configuration files are stored in platform-appropriate locations:

- **Linux/Unix:** `~/.config/mybible-get/`
- **macOS:** `~/Library/Application Support/mybible-get/`
- **Windows:** `%APPDATA%\mybible-get\`

### Configuration Structure

```
mybible-get/
├── config.json          # Module path configuration
├── cache.db             # SQLite cache of available modules
├── etags.json           # HTTP ETag cache for efficient updates
├── sources/             # Registry source URLs
│   ├── mybible.zone.registry
│   └── myb.1gb.ru.registry
└── .cache/              # Temporary download and registry cache
    ├── downloads/       # Downloaded module files
    └── registries/      # Cached registry files
```

## Module Types

The tool automatically detects and categorizes modules by type:

- **bible** - Bible translations
- **commentaries** - Biblical commentaries
- **dictionaries** - Biblical dictionaries and lexicons
- **devotions** - Devotional materials
- **cross-references** - Cross-reference data
- **subheadings** - Subheading data

## Integration

This tool is designed to work alongside other biblical text processing applications. The installed modules can be used with:

- Custom biblical text processors
- Translation tools
- Text analysis pipelines
- Other MyBible-compatible applications

## License

This project is licensed under the **Apache License 2.0**.

**Important**: This license applies only to the `mybible-get` tool itself. The modules, the biblical text and other content they contain, the MyBible name, and the MyBible module format are all subject to their respective licenses and copyright holders, and are not covered by this license. Users are responsible for ensuring compliance with all applicable licenses for any content downloaded with this tool.



## Related Projects
- [mybible-cli](https://github.com/kosivantsov/mybible-cli-java) - Java CLI/GUI tool for retrieving biblical text from MyBible modules.

---

**Note:** This is an auxiliary tool for module management. For actual biblical text reading and study, use appropriate Bible study software or text processing tools.


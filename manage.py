#!/usr/bin/env python3
"""
TuringMind Arena — Management CLI

Usage:
    python manage.py vendor-deps       Download JS/font dependencies for offline use
    python manage.py status            Show system status
    python manage.py health            Run health checks
    python manage.py sync              Sync turingskill repo and re-slice avatars
    python manage.py lint-wiki         Run wiki lint checks
    python manage.py export <sid>      Export a session (md or json)
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import sys

if sys.version_info < (3, 11):
    try:
        import tomli as tomllib
    except ImportError:
        print("Python < 3.11 detected. Install tomli: pip install tomli")
        print("Or use: uv run python manage.py <command>")
        sys.exit(1)
else:
    import tomllib
from pathlib import Path

BASE_DIR = Path(__file__).parent

# ── Vendor dependencies ──────────────────────────────────────

VENDOR_DEPS = {
    "d3.min.js": {
        "url": "https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js",
        "sha256": None,  # will be set after first download
        "description": "d3.js v7.9.0 — force-directed graph layout",
    },
    "marked.min.js": {
        "url": "https://cdnjs.cloudflare.com/ajax/libs/marked/15.0.7/marked.min.js",
        "sha256": None,
        "description": "marked.js v15 — markdown to HTML renderer",
    },
}

VENDOR_FONTS = {
    "Playfair Display": {
        "weights": "400;600;700",
        "url": "https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&display=swap",
    },
    "Source Sans 3": {
        "weights": "300;400;500;600",
        "url": "https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;500;600&display=swap",
    },
    "JetBrains Mono": {
        "weights": "400;500",
        "url": "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap",
    },
}


def cmd_vendor_deps(args):
    """Download JS dependencies and fonts for offline/local use."""
    import urllib.request

    vendor_dir = BASE_DIR / "static" / "vendor"
    vendor_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading JS dependencies...")
    for filename, info in VENDOR_DEPS.items():
        target = vendor_dir / filename
        print(f"  {filename}: {info['url']}")
        try:
            urllib.request.urlretrieve(info["url"], str(target))
            size = target.stat().st_size
            sha = hashlib.sha256(target.read_bytes()).hexdigest()[:16]
            print(f"    ✓ {size:,} bytes (sha256: {sha})")
        except Exception as e:
            print(f"    ✗ Failed: {e}")

    # Download Google Fonts CSS (contains @font-face declarations with woff2 URLs)
    fonts_dir = vendor_dir / "fonts"
    fonts_dir.mkdir(exist_ok=True)
    print("\nDownloading font CSS...")
    for name, info in VENDOR_FONTS.items():
        safe_name = name.lower().replace(" ", "-")
        css_path = fonts_dir / f"{safe_name}.css"
        print(f"  {name}: {info['url']}")
        try:
            req = urllib.request.Request(info["url"], headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
            })
            with urllib.request.urlopen(req) as resp:
                css = resp.read().decode("utf-8")
            css_path.write_text(css)
            # Extract and download woff2 files
            import re
            woff_urls = re.findall(r'url\((https://[^)]+\.woff2)\)', css)
            for wurl in woff_urls:
                woff_name = wurl.split("/")[-1]
                woff_path = fonts_dir / woff_name
                if not woff_path.exists():
                    urllib.request.urlretrieve(wurl, str(woff_path))
                    print(f"    ✓ {woff_name} ({woff_path.stat().st_size:,} bytes)")
                # Rewrite CSS to use local path
                css = css.replace(wurl, f"/static/vendor/fonts/{woff_name}")
            css_path.write_text(css)
            print(f"    ✓ CSS saved ({len(css):,} chars)")
        except Exception as e:
            print(f"    ✗ Failed: {e}")

    # Write manifest
    manifest = {
        "vendored_at": __import__("datetime").datetime.now().isoformat(),
        "js": {k: {"file": k, "size": (vendor_dir / k).stat().st_size}
               for k in VENDOR_DEPS if (vendor_dir / k).exists()},
        "fonts": {k: {"css": f"fonts/{k.lower().replace(' ', '-')}.css"}
                  for k in VENDOR_FONTS if (fonts_dir / f"{k.lower().replace(' ', '-')}.css").exists()},
    }
    (vendor_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest written to {vendor_dir / 'manifest.json'}")
    print("\nDone! The app will now use local files instead of CDN.")
    print("Restart the server to apply changes.")


# ── Status ──────────────────────────────────────────────────

def cmd_status(args):
    """Show system status."""
    config_path = BASE_DIR / "config.toml"
    print("=== TuringMind Arena Status ===\n")

    # Config
    if config_path.exists():
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
        default_prov = config.get("default", {}).get("provider", "?")
        providers = list(config.get("providers", {}).keys())
        print(f"Config:       {config_path}")
        print(f"Default LLM:  {default_prov}")
        print(f"Providers:    {', '.join(providers)}")
    else:
        print(f"Config:       MISSING ({config_path})")

    # Turingskill data
    ts_path = BASE_DIR / config.get("data", {}).get("turingskill_path", "turingskill")
    skill_count = len(list(ts_path.glob("*/SKILL.md"))) if ts_path.exists() else 0
    print(f"\nSkill data:   {ts_path} ({'✓' if ts_path.exists() else '✗ MISSING'})")
    print(f"SKILL.md:     {skill_count} files")

    # Avatars
    avatar_dir = BASE_DIR / "static" / "img" / "avatars"
    avatar_count = len(list(avatar_dir.glob("*.png"))) if avatar_dir.exists() else 0
    print(f"\nAvatars:      {avatar_count} PNG files")

    # Vendor deps
    vendor_dir = BASE_DIR / "static" / "vendor"
    d3_local = (vendor_dir / "d3.min.js").exists()
    marked_local = (vendor_dir / "marked.min.js").exists()
    print(f"\nVendor deps:")
    print(f"  d3.js:      {'✓ local' if d3_local else '→ CDN'}")
    print(f"  marked.js:  {'✓ local' if marked_local else '→ CDN (or disabled)'}")

    # Database
    db_path = BASE_DIR / config.get("data", {}).get("duckdb_path", "turingmind.duckdb")
    if db_path.exists():
        import duckdb
        conn = duckdb.connect(str(db_path), read_only=True)
        tables = {}
        for t in ["sessions", "messages", "wiki_pages", "wiki_links", "topics", "user_collection"]:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                tables[t] = count
            except Exception:
                tables[t] = "?"
        conn.close()
        print(f"\nDatabase:     {db_path} ({db_path.stat().st_size / 1024:.0f} KB)")
        for t, c in tables.items():
            print(f"  {t:20s} {c}")
    else:
        print(f"\nDatabase:     not yet created (will auto-create on first run)")

    print()


# ── Health ──────────────────────────────────────────────────

def cmd_health(args):
    """Run health checks."""
    issues = []
    ok_count = 0

    def check(name, condition, fix=""):
        nonlocal ok_count
        if condition:
            print(f"  ✓ {name}")
            ok_count += 1
        else:
            msg = f"  ✗ {name}"
            if fix:
                msg += f" — fix: {fix}"
            print(msg)
            issues.append(name)

    print("=== Health Check ===\n")

    # Config
    config_path = BASE_DIR / "config.toml"
    check("config.toml exists", config_path.exists(), "cp config.toml.example config.toml")

    if config_path.exists():
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
        from server import validate_config
        errors = validate_config(config)
        check("config.toml valid", not errors, "; ".join(errors))

        # Check API keys aren't placeholder
        for name, prov in config.get("providers", {}).items():
            key = prov.get("api_key", "")
            is_placeholder = "YOUR" in key.upper() or key == "dummy" or len(key) < 10
            if name != "local_vllm":  # local doesn't need real key
                check(f"provider '{name}' has API key", not is_placeholder, f"set api_key in [providers.{name}]")
    else:
        config = {}

    # Python deps
    for mod in ["tornado", "duckdb", "httpx"]:
        try:
            __import__(mod)
            check(f"python: {mod} installed", True)
        except ImportError:
            check(f"python: {mod} installed", False, f"pip install {mod}")

    # Turingskill
    ts_path = BASE_DIR / config.get("data", {}).get("turingskill_path", "turingskill")
    check("turingskill data present", ts_path.exists() and (ts_path / "SKILL.md").exists(),
          "git clone https://github.com/yfyang86/turingskill.git turingskill")

    # Avatars
    avatar_dir = BASE_DIR / "static" / "img" / "avatars"
    avatar_count = len(list(avatar_dir.glob("*-64.png"))) if avatar_dir.exists() else 0
    check(f"avatars present ({avatar_count}/81)", avatar_count >= 81,
          "python utils/slice_avatars.py <grid_image.png>")

    # Vendor deps
    vendor_dir = BASE_DIR / "static" / "vendor"
    check("d3.js vendored locally", (vendor_dir / "d3.min.js").exists(),
          "python manage.py vendor-deps")
    check("marked.js vendored locally", (vendor_dir / "marked.min.js").exists(),
          "python manage.py vendor-deps")

    # DB writable
    db_path = BASE_DIR / config.get("data", {}).get("duckdb_path", "turingmind.duckdb")
    db_dir = db_path.parent
    check("database directory writable", os.access(str(db_dir), os.W_OK))

    # Server port
    import socket
    port = int(os.environ.get("PORT", 8888))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_free = sock.connect_ex(("localhost", port)) != 0
    sock.close()
    check(f"port {port} available", port_free, f"another process is using port {port}")

    print(f"\n{ok_count} passed, {len(issues)} issues")
    if issues:
        print("Issues:", ", ".join(issues))
    return len(issues) == 0


# ── Sync ──────────────────────────────────────────────────

def cmd_sync(args):
    """Sync turingskill repo and re-slice avatars."""
    config_path = BASE_DIR / "config.toml"
    if config_path.exists():
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
    else:
        config = {}

    ts_path = BASE_DIR / config.get("data", {}).get("turingskill_path", "turingskill")

    # Git pull
    print("=== Syncing turingskill data ===")
    if ts_path.exists() and (ts_path / ".git").exists():
        print(f"  git pull in {ts_path}")
        result = subprocess.run(["git", "-C", str(ts_path), "pull", "--rebase"],
                                capture_output=True, text=True)
        print(f"  {result.stdout.strip()}")
        if result.returncode != 0:
            print(f"  Warning: {result.stderr.strip()}")
    elif not ts_path.exists():
        print(f"  Cloning to {ts_path}")
        subprocess.run(["git", "clone", "https://github.com/yfyang86/turingskill.git", str(ts_path)])
    else:
        print(f"  {ts_path} exists but is not a git repo — skipping pull")

    # Re-slice avatars if grid image exists
    grid_images = list(BASE_DIR.glob("*turing*1080*1080*.png")) + list(BASE_DIR.glob("*turing*grid*.png"))
    if grid_images:
        print(f"\n  Re-slicing avatars from {grid_images[0].name}")
        subprocess.run([sys.executable, str(BASE_DIR / "utils" / "slice_avatars.py"),
                        str(grid_images[0]), "-o", str(BASE_DIR / "static" / "img" / "avatars")])
    else:
        print("\n  No grid image found — skipping avatar slice")
        print("  Place a 9×9 grid image (1080×1080) in the project root and re-run")

    print("\nSync complete. Restart the server to pick up changes.")


# ── Wiki lint ──────────────────────────────────────────────

def cmd_lint_wiki(args):
    """Run wiki lint and display results."""
    config_path = BASE_DIR / "config.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    from core.session_manager import SessionManager
    from core.agent_manager import AgentManager
    from core.wiki_engine import WikiEngine

    data_cfg = config.get("data", {})
    sm = SessionManager(data_cfg.get("duckdb_path", "turingmind.duckdb"))
    am = AgentManager(data_cfg.get("turingskill_path", "./turingskill"))
    wiki = WikiEngine(sm, am)

    stats = wiki.stats()
    print(f"Wiki: {stats['pages']} pages, {stats['links']} links, {stats['timeline_entries']} timeline entries\n")

    result = wiki.lint()
    if not result["issues"]:
        print("✓ No issues found")
    else:
        for issue in result["issues"]:
            itype = issue["type"].upper()
            title = issue.get("title", issue.get("slug", issue.get("to", "")))
            suggestion = issue.get("suggestion", "")
            print(f"  [{itype}] {title}")
            if suggestion:
                print(f"    → {suggestion}")
    print(f"\n{len(result['issues'])} issue(s)")


# ── Export ──────────────────────────────────────────────────

def cmd_export(args):
    """Export a session."""
    config_path = BASE_DIR / "config.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    from core.session_manager import SessionManager
    sm = SessionManager(config.get("data", {}).get("duckdb_path", "turingmind.duckdb"))

    if args.session_id == "list":
        sessions = sm.list_sessions()
        for s in sessions:
            print(f"  {s['id']}  {s['name']}  ({s['updated_at'][:10]})")
        return

    fmt = args.format or "md"
    if fmt == "md":
        content = sm.export_session_md(args.session_id)
        if not content:
            print(f"Session '{args.session_id}' not found")
            return
        out = BASE_DIR / f"session-{args.session_id}.md"
        out.write_text(content)
        print(f"Exported to {out}")
    else:
        data = sm.export_session_json(args.session_id)
        out = BASE_DIR / f"session-{args.session_id}.json"
        out.write_text(json.dumps(data, indent=2))
        print(f"Exported to {out}")


# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="TuringMind Arena management tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  vendor-deps    Download JS/font dependencies for offline use
  status         Show system status (config, DB, avatars, deps)
  health         Run health checks with fix suggestions
  sync           Sync turingskill repo + re-slice avatars
  lint-wiki      Run wiki health checks
  export         Export session (use 'export list' to see sessions)
""",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("vendor-deps", help="Download dependencies for offline use")
    sub.add_parser("status", help="Show system status")
    sub.add_parser("health", help="Run health checks")
    sub.add_parser("sync", help="Sync turingskill data + avatars")
    sub.add_parser("lint-wiki", help="Run wiki lint checks")

    p_export = sub.add_parser("export", help="Export a session")
    p_export.add_argument("session_id", help="Session ID (or 'list')")
    p_export.add_argument("--format", "-f", choices=["md", "json"], default="md")

    args = parser.parse_args()

    commands = {
        "vendor-deps": cmd_vendor_deps,
        "status": cmd_status,
        "health": cmd_health,
        "sync": cmd_sync,
        "lint-wiki": cmd_lint_wiki,
        "export": cmd_export,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

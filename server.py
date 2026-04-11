#!/usr/bin/env python3
"""TuringMind Arena — main entry point. Config validation, logging, hot-reload."""

from __future__ import annotations
import logging
import os
import sys
import tomllib
from pathlib import Path

import tornado.ioloop
import tornado.web

from core.llm_router import LLMRouter
from core.agent_manager import AgentManager
from core.session_manager import SessionManager
from core.wiki_engine import WikiEngine
from handlers.chat import ChatWSHandler
from handlers.upload import FileUploadHandler
from handlers.api import (
    SessionListHandler, SessionHandler, SessionLaureateHandler,
    LaureateListHandler, LaureateByEraHandler, LaureateSearchHandler,
    AvatarHandler, MessageListHandler, MessageSearchHandler,
    CollectionHandler, UserStyleHandler, ExportHandler,
    TopicListHandler, TopicAddHandler, TopicTimelineHandler,
    RecommendHandler, PresetsHandler,
)
from handlers.wiki import (
    WikiStatsHandler, WikiIngestHandler, WikiIngestAllHandler,
    WikiPagesHandler, WikiPageHandler, WikiSearchHandler,
    WikiLogHandler, WikiLintHandler, WikiConceptTimelineHandler,
    WikiGraphHandler, WikiPageHistoryHandler, WikiPageDiffHandler,
    WikiContradictionsHandler, WikiRelatedHandler, WikiConsolidateHandler,
    WikiGenerateHandler, WikiDisambiguateHandler, WikiAddPageHandler,
)

BASE_DIR = Path(__file__).parent
log = logging.getLogger(__name__)


# L29: Structured logging
def setup_logging(debug: bool = False):
    level = logging.DEBUG if debug else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%Y-%m-%d %H:%M:%S")
    # Quiet noisy libs
    logging.getLogger("tornado.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


# L28: Config validation
def validate_config(config: dict) -> list[str]:
    errors = []
    if "default" not in config:
        errors.append("Missing [default] section")
    elif "provider" not in config.get("default", {}):
        errors.append("Missing default.provider")
    providers = config.get("providers", {})
    if not providers:
        errors.append("No providers configured in [providers]")
    default_name = config.get("default", {}).get("provider", "")
    if default_name and default_name not in providers:
        errors.append(f"Default provider '{default_name}' not found in [providers]")
    for name, prov in providers.items():
        if "api_base" not in prov:
            errors.append(f"Provider '{name}' missing api_base")
        if "model" not in prov:
            errors.append(f"Provider '{name}' missing model")
        if "api_key" not in prov:
            errors.append(f"Provider '{name}' missing api_key")
    return errors


def load_config() -> dict:
    config_path = BASE_DIR / "config.toml"
    if not config_path.exists():
        print(f"ERROR: {config_path} not found.")
        sys.exit(1)
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    errors = validate_config(config)
    if errors:
        print("Config validation errors:")
        for e in errors:
            print(f"  - {e}")
        print("Fix config.toml and restart.")
        sys.exit(1)
    return config


def make_app(config: dict) -> tornado.web.Application:
    data_cfg = config.get("data", {})
    llm_router = LLMRouter(config)
    agent_manager = AgentManager(data_cfg.get("turingskill_path", "./turingskill"))
    session_manager = SessionManager(data_cfg.get("duckdb_path", "./turingmind.duckdb"))
    wiki_engine = WikiEngine(session_manager, agent_manager)

    app = tornado.web.Application(
        [
            (r"/", MainHandler),
            (r"/ws/chat", ChatWSHandler),
            # Sessions
            (r"/api/sessions", SessionListHandler),
            (r"/api/sessions/([^/]+)", SessionHandler),
            (r"/api/sessions/([^/]+)/laureates", SessionLaureateHandler),
            (r"/api/sessions/([^/]+)/messages", MessageListHandler),
            (r"/api/sessions/([^/]+)/export", ExportHandler),
            # Laureates
            (r"/api/laureates", LaureateListHandler),
            (r"/api/laureates/search", LaureateSearchHandler),
            (r"/api/laureates/by-era", LaureateByEraHandler),
            (r"/api/laureates/recommend", RecommendHandler),
            (r"/api/laureates/presets", PresetsHandler),
            (r"/api/avatar/([^/]+)", AvatarHandler),
            # Features
            (r"/api/upload", FileUploadHandler),
            (r"/api/collection", CollectionHandler),
            (r"/api/user-style", UserStyleHandler),
            (r"/api/messages/search", MessageSearchHandler),
            (r"/api/topics", TopicListHandler),
            (r"/api/topics/add", TopicAddHandler),
            (r"/api/topics/timeline", TopicTimelineHandler),
            # Wiki
            (r"/api/wiki/stats", WikiStatsHandler),
            (r"/api/wiki/ingest", WikiIngestHandler),
            (r"/api/wiki/ingest-all", WikiIngestAllHandler),
            (r"/api/wiki/pages", WikiPagesHandler),
            (r"/api/wiki/pages/([^/]+)", WikiPageHandler),
            (r"/api/wiki/search", WikiSearchHandler),
            (r"/api/wiki/log", WikiLogHandler),
            (r"/api/wiki/lint", WikiLintHandler),
            (r"/api/wiki/concept-timeline/([^/]+)", WikiConceptTimelineHandler),
            (r"/api/wiki/graph", WikiGraphHandler),
            (r"/api/wiki/pages/([^/]+)/history", WikiPageHistoryHandler),
            (r"/api/wiki/pages/([^/]+)/diff", WikiPageDiffHandler),
            (r"/api/wiki/contradictions", WikiContradictionsHandler),
            (r"/api/wiki/related", WikiRelatedHandler),
            (r"/api/wiki/consolidate", WikiConsolidateHandler),
            (r"/api/wiki/generate", WikiGenerateHandler),
            (r"/api/wiki/disambiguate", WikiDisambiguateHandler),
            (r"/api/wiki/add", WikiAddPageHandler),
        ],
        template_path=str(BASE_DIR / "templates"),
        static_path=str(BASE_DIR / "static"),
        debug=True,
        config=config,
        llm_router=llm_router,
        agent_manager=agent_manager,
        session_manager=session_manager,
        wiki_engine=wiki_engine,
    )

    # L32: Config hot-reload via file watcher
    config_path = BASE_DIR / "config.toml"
    _config_mtime = [config_path.stat().st_mtime if config_path.exists() else 0]

    def check_config_reload():
        try:
            mt = config_path.stat().st_mtime
            if mt > _config_mtime[0]:
                _config_mtime[0] = mt
                with open(config_path, "rb") as f:
                    new_config = tomllib.load(f)
                errs = validate_config(new_config)
                if not errs:
                    app.settings["config"] = new_config
                    llm_router.reload_config(new_config)
                    log.info("Config reloaded successfully")
                else:
                    log.warning("Config reload failed: %s", errs)
        except Exception as e:
            log.warning("Config reload check error: %s", e)

    tornado.ioloop.PeriodicCallback(check_config_reload, 5000).start()

    return app


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")


def main():
    config = load_config()
    debug = config.get("data", {}).get("debug", True)
    setup_logging(debug)
    port = int(os.environ.get("PORT", 8888))
    app = make_app(config)
    app.listen(port)
    log.info("🏆 TuringMind Arena running at http://localhost:%d", port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()

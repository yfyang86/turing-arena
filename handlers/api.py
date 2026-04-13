"""REST API handlers. Collection gallery, export, message search, topics, recommend."""

from __future__ import annotations
import json
import os
import tornado.web
from core.avatar_gen import generate_avatar_svg


class BaseAPIHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
    def write_json(self, data):
        self.write(json.dumps(data))
    def parse_body(self) -> dict | None:
        """Safely parse JSON request body. Returns None on error (sends 400)."""
        try:
            return json.loads(self.request.body)
        except (json.JSONDecodeError, TypeError):
            self.set_status(400)
            self.write_json({"error": "Invalid JSON body"})
            return None

# --- Sessions ---
class SessionListHandler(BaseAPIHandler):
    def get(self): self.write_json(self.application.settings["session_manager"].list_sessions())
    def post(self):
        body = self.parse_body()
        if body is None: return
        self.write_json(self.application.settings["session_manager"].create_session(body.get("name", "New Arena")))

class SessionHandler(BaseAPIHandler):
    def delete(self, sid):
        self.application.settings["session_manager"].delete_session(sid)
        self.write_json({"ok": True})
    def patch(self, sid):
        body = self.parse_body()
        if body is None: return
        if "name" in body:
            self.application.settings["session_manager"].rename_session(sid, body["name"])
        self.write_json({"ok": True})

class SessionLaureateHandler(BaseAPIHandler):
    def get(self, sid):
        sm, am = self.application.settings["session_manager"], self.application.settings["agent_manager"]
        self.write_json([am.get(s).to_dict() for s in sm.get_session_laureates(sid) if am.get(s)])

    def post(self, sid):
        body = self.parse_body()
        if body is None: return
        slug = body.get("slug")
        sm = self.application.settings["session_manager"]
        max_l = self.application.settings["config"].get("arena", {}).get("max_laureates_per_room", 5)
        current = sm.get_session_laureates(sid)
        if len(current) >= max_l and slug not in current:
            self.set_status(400); self.write_json({"error": f"Max {max_l} laureates per room"}); return
        sm.add_laureate(sid, slug)
        self.write_json({"ok": True})

    def delete(self, sid):
        body = self.parse_body()
        if body is None: return
        self.application.settings["session_manager"].remove_laureate(sid, body.get("slug"))
        self.write_json({"ok": True})

# --- Laureates ---
class LaureateListHandler(BaseAPIHandler):
    def get(self): self.write_json(self.application.settings["agent_manager"].list_all())

class LaureateSearchHandler(BaseAPIHandler):
    def get(self):
        q = self.get_argument("q", "")
        self.write_json(self.application.settings["agent_manager"].search(q, int(self.get_argument("limit", "20"))))

class LaureateByEraHandler(BaseAPIHandler):
    def get(self): self.write_json(self.application.settings["agent_manager"].list_by_era())

# L12: Topic-match recommend
class RecommendHandler(BaseAPIHandler):
    def get(self):
        topic = self.get_argument("topic", "")
        limit = int(self.get_argument("limit", "3"))
        self.write_json(self.application.settings["agent_manager"].recommend_for_topic(topic, limit))

# L13: Era Clash / Thinking Duel presets
class PresetsHandler(BaseAPIHandler):
    def get(self):
        mode = self.get_argument("mode", "era_clash")
        am = self.application.settings["agent_manager"]
        if mode == "thinking_duel":
            self.write_json(am.thinking_duel())
        else:
            self.write_json(am.era_clash())

class AvatarHandler(tornado.web.RequestHandler):
    # Map request size → file suffix. Always serve >= requested so browser downscales (sharper).
    SIZE_MAP = {
        24: "32", 28: "32", 32: "32",
        36: "64", 48: "64", 64: "64",
        80: "96", 96: "96",
        128: "full",
    }

    def get(self, slug):
        am = self.application.settings["agent_manager"]
        agent = am.get(slug)
        if not agent: self.set_status(404); self.write("Not found"); return

        size = int(self.get_argument("size", "64"))
        self.set_header("Cache-Control", "public, max-age=86400")

        # Try PNG portrait first
        suffix = self.SIZE_MAP.get(size)
        if suffix is None:
            # Find closest
            suffix = "64"
            for s, sf in sorted(self.SIZE_MAP.items()):
                if s >= size: suffix = sf; break

        avatar_dir = os.path.join(self.application.settings["static_path"], "img", "avatars")
        if suffix == "full":
            png_path = os.path.join(avatar_dir, f"{slug}.png")
        else:
            png_path = os.path.join(avatar_dir, f"{slug}-{suffix}.png")

        if os.path.exists(png_path):
            self.set_header("Content-Type", "image/png")
            with open(png_path, "rb") as f:
                self.write(f.read())
        else:
            # Fallback to generated SVG
            self.set_header("Content-Type", "image/svg+xml")
            self.write(generate_avatar_svg(agent.initials, agent.era, agent.year, size))

# --- Messages ---
class MessageListHandler(BaseAPIHandler):
    def get(self, sid): self.write_json(self.application.settings["session_manager"].get_messages(sid))

# L19: Message search
class MessageSearchHandler(BaseAPIHandler):
    def get(self):
        q = self.get_argument("q", "")
        sid = self.get_argument("session_id", None)
        self.write_json(self.application.settings["session_manager"].search_messages(q, sid))

# --- Collection (L14) ---
class CollectionHandler(BaseAPIHandler):
    def get(self):
        sm = self.application.settings["session_manager"]
        am = self.application.settings["agent_manager"]
        collection = sm.get_collection()
        # Enrich with laureate info
        for item in collection:
            agent = am.get(item["slug"])
            if agent:
                item.update(agent.to_dict())
                item["style"] = am.get_thinking_style(item["slug"])
        self.write_json(collection)

# L15: User thinking style
class UserStyleHandler(BaseAPIHandler):
    def get(self):
        sm = self.application.settings["session_manager"]
        am = self.application.settings["agent_manager"]
        stats = sm.get_user_style_stats()
        style_counts: dict[str, int] = {}
        for slug, count in stats.items():
            style = am.get_thinking_style(slug)
            if style:
                style_counts[style] = style_counts.get(style, 0) + count
        dominant = max(style_counts, key=style_counts.get) if style_counts else None
        self.write_json({"styles": style_counts, "dominant": dominant, "top_laureates": stats})

# L18: Export
class ExportHandler(BaseAPIHandler):
    def get(self, sid):
        fmt = self.get_argument("format", "json")
        sm = self.application.settings["session_manager"]
        if fmt == "md":
            self.set_header("Content-Type", "text/markdown")
            self.set_header("Content-Disposition", f'attachment; filename="session-{sid}.md"')
            self.write(sm.export_session_md(sid))
        else:
            self.write_json(sm.export_session_json(sid))

# --- Topics (L9/L10/L11) ---
class TopicListHandler(BaseAPIHandler):
    def get(self):
        self.write_json(self.application.settings["session_manager"].get_topics())

class TopicAddHandler(BaseAPIHandler):
    def post(self):
        body = self.parse_body()
        if body is None: return
        name = body.get("name", "").strip().lower()
        if not name:
            self.set_status(400); self.write_json({"error": "name required"}); return
        sm = self.application.settings["session_manager"]
        topic_id = sm.add_topic(name)
        # Also create a wiki concept page if it doesn't exist
        wiki = self.application.settings["wiki_engine"]
        from core.wiki_engine import _slugify
        slug = f"concept-{_slugify(name)}"
        if not wiki.get_page(slug):
            wiki.create_manual_page(name, "concept")
        self.write_json({"id": topic_id, "name": name, "slug": slug})

class TopicTimelineHandler(BaseAPIHandler):
    def get(self):
        name = self.get_argument("name", "")
        self.write_json(self.application.settings["session_manager"].get_topic_timeline(name))

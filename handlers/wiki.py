"""Wiki REST API handlers."""

from __future__ import annotations
import json
import tornado.web


class BaseWikiHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
    def write_json(self, data):
        self.write(json.dumps(data))
    @property
    def wiki(self):
        return self.application.settings["wiki_engine"]


class WikiStatsHandler(BaseWikiHandler):
    def get(self):
        self.write_json(self.wiki.stats())


class WikiIngestHandler(BaseWikiHandler):
    async def post(self):
        body = json.loads(self.request.body)
        session_id = body.get("session_id")
        force = body.get("force", False)
        use_llm = body.get("use_llm", False)
        if not session_id:
            self.set_status(400); self.write_json({"error": "session_id required"}); return
        if use_llm:
            llm = self.application.settings["llm_router"]
            result = await self.wiki.ingest_session_llm(session_id, llm, force=force)
        else:
            result = self.wiki.ingest_session(session_id, force=force)
        if "error" in result:
            self.set_status(400)
        self.write_json(result)


class WikiIngestAllHandler(BaseWikiHandler):
    async def post(self):
        """Ingest all sessions."""
        sm = self.application.settings["session_manager"]
        sessions = sm.list_sessions()
        use_llm = False
        try:
            body = json.loads(self.request.body) if self.request.body else {}
            use_llm = body.get("use_llm", False)
        except: pass
        llm = self.application.settings["llm_router"] if use_llm else None
        results = []
        for s in sessions:
            if use_llm and llm:
                result = await self.wiki.ingest_session_llm(s["id"], llm)
            else:
                result = self.wiki.ingest_session(s["id"])
            results.append(result)
        total_created = sum(r.get("pages_created", 0) for r in results)
        total_updated = sum(r.get("pages_updated", 0) for r in results)
        self.write_json({
            "sessions_processed": len(results),
            "total_pages_created": total_created,
            "total_pages_updated": total_updated,
            "llm_enhanced": use_llm,
            "details": results,
        })


class WikiAddPageHandler(BaseWikiHandler):
    def post(self):
        body = json.loads(self.request.body)
        title = body.get("title", "").strip()
        page_type = body.get("page_type", "concept")
        if not title:
            self.set_status(400); self.write_json({"error": "title required"}); return
        result = self.wiki.create_manual_page(title, page_type)
        if "error" in result:
            self.set_status(409)
        self.write_json(result)


class WikiPagesHandler(BaseWikiHandler):
    def get(self):
        page_type = self.get_argument("type", None)
        self.write_json(self.wiki.list_pages(page_type))


class WikiPageHandler(BaseWikiHandler):
    def get(self, slug):
        page = self.wiki.get_page(slug)
        if not page:
            self.set_status(404); self.write_json({"error": "Page not found"}); return
        links = self.wiki.get_links(slug)
        page["links"] = links
        self.write_json(page)

    def delete(self, slug):
        self.wiki.delete_page(slug)
        self.write_json({"ok": True})


class WikiSearchHandler(BaseWikiHandler):
    def get(self):
        q = self.get_argument("q", "")
        self.write_json(self.wiki.search_pages(q))


class WikiLogHandler(BaseWikiHandler):
    def get(self):
        self.write_json(self.wiki.get_log(int(self.get_argument("limit", "50"))))


class WikiLintHandler(BaseWikiHandler):
    def get(self):
        self.write_json(self.wiki.lint())


class WikiConceptTimelineHandler(BaseWikiHandler):
    def get(self, slug):
        self.write_json(self.wiki.get_concept_timeline(slug))


class WikiGraphHandler(BaseWikiHandler):
    def get(self):
        focus = self.get_argument("focus", None)
        hops = int(self.get_argument("hops", "1"))
        self.write_json(self.wiki.graph_data(focus=focus, hops=hops))


class WikiPageHistoryHandler(BaseWikiHandler):
    def get(self, slug):
        self.write_json(self.wiki.get_page_history(slug))


class WikiPageDiffHandler(BaseWikiHandler):
    def get(self, slug):
        v1 = int(self.get_argument("v1", "1"))
        v2 = int(self.get_argument("v2", "2"))
        self.write_json(self.wiki.diff_pages(slug, v1, v2))


class WikiContradictionsHandler(BaseWikiHandler):
    def get(self):
        self.write_json(self.wiki.detect_contradictions())


class WikiRelatedHandler(BaseWikiHandler):
    def get(self):
        ignore_raw = self.get_argument("ignore", "")
        ignore = [x.strip() for x in ignore_raw.split(",") if x.strip()] if ignore_raw else None
        self.write_json(self.wiki.find_related_concepts(ignore=ignore))


class WikiConsolidateHandler(BaseWikiHandler):
    def post(self):
        body = json.loads(self.request.body)
        primary = body.get("primary")
        merge = body.get("merge", [])
        if not primary or not merge:
            self.set_status(400); self.write_json({"error": "primary and merge[] required"}); return
        self.write_json(self.wiki.consolidate_concepts(primary, merge))


class WikiGenerateHandler(BaseWikiHandler):
    async def post(self):
        body = json.loads(self.request.body)
        slug = body.get("slug")
        if not slug:
            self.set_status(400); self.write_json({"error": "slug required"}); return
        llm = self.application.settings["llm_router"]
        result = await self.wiki.generate_structured_page(slug, llm)
        if "error" in result:
            self.set_status(400)
        self.write_json(result)


class WikiDisambiguateHandler(BaseWikiHandler):
    def post(self):
        body = json.loads(self.request.body)
        term = body.get("term")
        entries = body.get("entries", [])
        if not term or not entries:
            self.set_status(400); self.write_json({"error": "term and entries[] required"}); return
        self.write_json(self.wiki.create_disambiguation_page(term, entries))

"""Session, message, topic persistence with DuckDB."""

from __future__ import annotations
import uuid
import json
import re
from datetime import datetime
from pathlib import Path
import duckdb


class SessionManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
        for stmt in schema_path.read_text().split(";"):
            stmt = stmt.strip()
            if stmt:
                self.conn.execute(stmt)

    # --- Sessions ---
    def create_session(self, name: str) -> dict:
        sid = str(uuid.uuid4())[:8]
        now = datetime.now().isoformat()
        self.conn.execute("INSERT INTO sessions VALUES (?,?,?,?)", [sid, name, now, now])
        return {"id": sid, "name": name, "created_at": now}

    def list_sessions(self) -> list[dict]:
        rows = self.conn.execute("SELECT id,name,created_at,updated_at FROM sessions ORDER BY updated_at DESC").fetchall()
        return [{"id": r[0], "name": r[1], "created_at": str(r[2]), "updated_at": str(r[3])} for r in rows]

    def delete_session(self, sid: str):
        for t in ("topic_mentions", "messages", "session_laureates", "sessions"):
            col = "session_id" if t != "sessions" else "id"
            self.conn.execute(f"DELETE FROM {t} WHERE {col} = ?", [sid])

    def rename_session(self, sid: str, name: str):
        self.conn.execute("UPDATE sessions SET name=?, updated_at=? WHERE id=?", [name, datetime.now().isoformat(), sid])

    def touch_session(self, sid: str):
        self.conn.execute("UPDATE sessions SET updated_at=? WHERE id=?", [datetime.now().isoformat(), sid])

    # --- Session Laureates ---
    def add_laureate(self, sid: str, slug: str):
        try:
            self.conn.execute("INSERT INTO session_laureates (session_id,laureate_slug) VALUES (?,?)", [sid, slug])
        except duckdb.ConstraintException:
            pass

    def remove_laureate(self, sid: str, slug: str):
        self.conn.execute("DELETE FROM session_laureates WHERE session_id=? AND laureate_slug=?", [sid, slug])

    def get_session_laureates(self, sid: str) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT laureate_slug FROM session_laureates WHERE session_id=? ORDER BY joined_at", [sid]).fetchall()]

    # --- Messages ---
    def add_message(self, sid: str, role: str, content: str,
                    laureate_slug: str | None = None, metadata: dict | None = None) -> dict:
        mid = str(uuid.uuid4())[:12]
        now = datetime.now().isoformat()
        self.conn.execute(
            "INSERT INTO messages VALUES (?,?,?,?,?,?,?)",
            [mid, sid, role, laureate_slug, content, json.dumps(metadata) if metadata else None, now])
        self.touch_session(sid)
        return {"id": mid, "session_id": sid, "role": role,
                "laureate_slug": laureate_slug, "content": content, "created_at": now}

    def get_messages(self, sid: str, limit: int = 200) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id,role,laureate_slug,content,metadata,created_at FROM messages WHERE session_id=? ORDER BY created_at ASC LIMIT ?",
            [sid, limit]).fetchall()
        results = []
        for r in rows:
            meta = None
            if r[4]:
                try: meta = json.loads(r[4])
                except: pass
            results.append({"id": r[0], "role": r[1], "laureate_slug": r[2],
                            "content": r[3], "metadata": meta, "created_at": str(r[5])})
        return results

    # L19: Message search
    def search_messages(self, query: str, session_id: str | None = None, limit: int = 50) -> list[dict]:
        q = f"%{query}%"
        if session_id:
            rows = self.conn.execute(
                "SELECT id,session_id,role,laureate_slug,content,created_at FROM messages WHERE session_id=? AND content ILIKE ? ORDER BY created_at DESC LIMIT ?",
                [session_id, q, limit]).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT id,session_id,role,laureate_slug,content,created_at FROM messages WHERE content ILIKE ? ORDER BY created_at DESC LIMIT ?",
                [q, limit]).fetchall()
        return [{"id": r[0], "session_id": r[1], "role": r[2], "laureate_slug": r[3],
                 "content": r[4][:200], "created_at": str(r[5])} for r in rows]

    # --- Collection (L16: unlock at 5+) ---
    UNLOCK_THRESHOLD = 5

    def increment_interaction(self, slug: str):
        existing = self.conn.execute("SELECT interaction_count FROM user_collection WHERE laureate_slug=?", [slug]).fetchone()
        if existing:
            self.conn.execute("UPDATE user_collection SET interaction_count=interaction_count+1 WHERE laureate_slug=?", [slug])
        else:
            self.conn.execute("INSERT INTO user_collection VALUES (?,?,1)", [slug, datetime.now().isoformat()])

    def get_collection(self) -> list[dict]:
        rows = self.conn.execute("SELECT laureate_slug,unlocked_at,interaction_count FROM user_collection ORDER BY interaction_count DESC").fetchall()
        return [{"slug": r[0], "unlocked_at": str(r[1]), "interactions": r[2],
                 "unlocked": r[2] >= self.UNLOCK_THRESHOLD} for r in rows]

    def get_interaction_count(self, slug: str) -> int:
        r = self.conn.execute("SELECT interaction_count FROM user_collection WHERE laureate_slug=?", [slug]).fetchone()
        return r[0] if r else 0

    # L15: Thinking style badge for user (based on most-interacted laureates)
    def get_user_style_stats(self) -> dict:
        rows = self.conn.execute(
            "SELECT laureate_slug, interaction_count FROM user_collection ORDER BY interaction_count DESC LIMIT 10"
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    # --- Topics (L9/L10) ---
    def add_topic(self, name: str) -> str:
        name_lower = name.lower().strip()
        existing = self.conn.execute("SELECT id FROM topics WHERE name=?", [name_lower]).fetchone()
        if existing:
            self.conn.execute("UPDATE topics SET mention_count=mention_count+1 WHERE id=?", [existing[0]])
            return existing[0]
        tid = str(uuid.uuid4())[:8]
        self.conn.execute("INSERT INTO topics VALUES (?,?,?,1)", [tid, name_lower, datetime.now().isoformat()])
        return tid

    def add_topic_mention(self, topic_id: str, message_id: str, session_id: str):
        mid = str(uuid.uuid4())[:8]
        self.conn.execute("INSERT INTO topic_mentions VALUES (?,?,?,?,?)",
                          [mid, topic_id, message_id, session_id, datetime.now().isoformat()])

    def get_topics(self, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id,name,first_seen,mention_count FROM topics ORDER BY mention_count DESC LIMIT ?", [limit]).fetchall()
        return [{"id": r[0], "name": r[1], "first_seen": str(r[2]), "mentions": r[3]} for r in rows]

    def get_topic_timeline(self, topic_name: str) -> list[dict]:
        """L10/L11: Get mention timestamps for hype cycle visualization."""
        rows = self.conn.execute("""
            SELECT tm.created_at, m.session_id, m.laureate_slug
            FROM topic_mentions tm
            JOIN topics t ON t.id = tm.topic_id
            JOIN messages m ON m.id = tm.message_id
            WHERE t.name = ?
            ORDER BY tm.created_at ASC
        """, [topic_name.lower().strip()]).fetchall()
        return [{"time": str(r[0]), "session": r[1], "laureate": r[2]} for r in rows]

    # L18: Export session
    def export_session_md(self, sid: str) -> str:
        session = self.conn.execute("SELECT name,created_at FROM sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return ""
        lines = [f"# {session[0]}", f"*Created: {session[1]}*\n"]
        for m in self.get_messages(sid):
            if m["role"] == "user":
                lines.append(f"**You:** {m['content']}\n")
            elif m["role"] == "laureate":
                lines.append(f"**{m['laureate_slug']}:** {m['content']}\n")
            else:
                lines.append(f"*{m['content']}*\n")
        return "\n".join(lines)

    def export_session_json(self, sid: str) -> dict:
        session = self.conn.execute("SELECT name,created_at FROM sessions WHERE id=?", [sid]).fetchone()
        laureates = self.get_session_laureates(sid)
        messages = self.get_messages(sid)
        return {"session": {"id": sid, "name": session[0] if session else "", "created_at": str(session[1]) if session else ""},
                "laureates": laureates, "messages": messages}

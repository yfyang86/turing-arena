"""
Wiki Engine — persistent knowledge base built from chat sessions.

Implements the LLM Wiki / Wiki RAG pattern:
- Ingest: session → extract entities/concepts → create/update wiki pages
- Index: auto-maintained catalog of all pages
- Log: chronological record of all operations
- Concept Timeline: time-tagged snapshots of how concepts evolve across sessions
- Lint: detect stale pages, orphans, contradictions
"""

from __future__ import annotations
import hashlib
import json
import re
import uuid
from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.session_manager import SessionManager
    from core.agent_manager import AgentManager

# ── Concept phase detection heuristics ──────────────────────────
PHASE_KEYWORDS = {
    "emerging":     ["new", "novel", "emerging", "proposed", "introducing", "first"],
    "established":  ["well-known", "standard", "established", "widely", "accepted", "proven"],
    "challenged":   ["however", "but", "challenge", "critique", "flaw", "limitation", "wrong", "disagree"],
    "revised":      ["revised", "updated", "new understanding", "reinterpret", "correction", "actually"],
    "deprecated":   ["obsolete", "replaced", "superseded", "outdated", "no longer"],
}


def detect_phase(text: str) -> str:
    """Detect the conceptual phase from message text."""
    tl = text.lower()
    scores = {}
    for phase, keywords in PHASE_KEYWORDS.items():
        scores[phase] = sum(1 for kw in keywords if kw in tl)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "mentioned"


# ── Concept/entity extraction (lightweight, no LLM) ────────────

def extract_entities_and_concepts(messages: list[dict], agent_manager) -> dict:
    """Extract laureate entities and concepts from session messages.
    Returns {entities: [{slug, name, mention_count, context}], concepts: [{name, count, contexts, phase}]}
    """
    entity_counts: Counter = Counter()
    entity_contexts: dict[str, list[str]] = {}
    concept_counts: Counter = Counter()
    concept_contexts: dict[str, list[str]] = {}
    concept_phases: dict[str, list[str]] = {}

    from core.topic_tracker import TOPIC_KEYWORDS

    for msg in messages:
        content = msg.get("content", "")
        # Entities: laureates mentioned in messages (by slug or as responders)
        slug = msg.get("laureate_slug")
        if slug:
            entity_counts[slug] += 1
            entity_contexts.setdefault(slug, []).append(content[:200])

        # Concepts: topic keyword matching
        content_lower = content.lower()
        for topic, keywords in TOPIC_KEYWORDS.items():
            for kw in keywords:
                if kw in content_lower:
                    concept_counts[topic] += 1
                    concept_contexts.setdefault(topic, []).append(content[:300])
                    concept_phases.setdefault(topic, []).append(detect_phase(content))
                    break

    entities = []
    for slug, count in entity_counts.most_common():
        agent = agent_manager.get(slug) if agent_manager else None
        entities.append({
            "slug": slug,
            "name": agent.name if agent else slug,
            "mention_count": count,
            "context": entity_contexts.get(slug, [""])[0][:200],
        })

    concepts = []
    for name, count in concept_counts.most_common():
        phases = concept_phases.get(name, [])
        # Dominant phase = most frequent
        phase_counter = Counter(phases)
        dominant = phase_counter.most_common(1)[0][0] if phase_counter else "mentioned"
        concepts.append({
            "name": name,
            "count": count,
            "contexts": concept_contexts.get(name, [])[:3],
            "phase": dominant,
        })

    return {"entities": entities, "concepts": concepts}


# ── Wiki page generation ────────────────────────────────────────

def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _content_hash(content: str, frontmatter: dict | None = None) -> str:
    """SHA-256 hash of page content + frontmatter. Git blob-style content addressing."""
    h = hashlib.sha256()
    h.update(content.encode("utf-8"))
    if frontmatter:
        # Stable JSON serialization (sorted keys) for deterministic hashing
        h.update(json.dumps(frontmatter, sort_keys=True).encode("utf-8"))
    return h.hexdigest()[:16]  # 16-char hex prefix (64-bit, sufficient for dedup)


def _session_content_hash(messages: list[dict]) -> str:
    """Hash of all message IDs + content in a session. Detects if session changed."""
    h = hashlib.sha256()
    for m in messages:
        h.update(m.get("id", "").encode("utf-8"))
        h.update(m.get("content", "").encode("utf-8"))
    return h.hexdigest()[:16]


def _simple_diff(lines_a: list[str], lines_b: list[str]) -> list[dict]:
    """Simple line-based diff using longest common subsequence. No external deps.
    Returns list of {op: 'equal'|'add'|'remove', text: str}."""
    m, n = len(lines_a), len(lines_b)

    # LCS table (optimize: only need two rows)
    if m > 500 or n > 500:
        # For very large files, fall back to simple sequential comparison
        return _sequential_diff(lines_a, lines_b)

    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m - 1, -1, -1):
        for j in range(n - 1, -1, -1):
            if lines_a[i] == lines_b[j]:
                dp[i][j] = dp[i + 1][j + 1] + 1
            else:
                dp[i][j] = max(dp[i + 1][j], dp[i][j + 1])

    # Backtrack to produce diff
    result = []
    i, j = 0, 0
    while i < m and j < n:
        if lines_a[i] == lines_b[j]:
            result.append({"op": "equal", "text": lines_a[i]})
            i += 1; j += 1
        elif dp[i + 1][j] >= dp[i][j + 1]:
            result.append({"op": "remove", "text": lines_a[i]})
            i += 1
        else:
            result.append({"op": "add", "text": lines_b[j]})
            j += 1
    while i < m:
        result.append({"op": "remove", "text": lines_a[i]}); i += 1
    while j < n:
        result.append({"op": "add", "text": lines_b[j]}); j += 1
    return result


def _sequential_diff(lines_a: list[str], lines_b: list[str]) -> list[dict]:
    """Fallback for large files: mark all of A as removed, all of B as added."""
    result = [{"op": "remove", "text": l} for l in lines_a]
    result += [{"op": "add", "text": l} for l in lines_b]
    return result


def _label_propagation(nodes: set[str], adj: dict[str, set[str]], max_iter: int = 20) -> dict[str, int]:
    """Simple label propagation for community detection. Pure Python, no deps.
    Each node starts with its own label. Iteratively adopts the most common
    label among its neighbors. Converges in ~5-10 iterations for small graphs."""
    import random
    # Initialize: each node gets unique label
    labels = {n: i for i, n in enumerate(sorted(nodes))}
    node_list = list(nodes)

    for _ in range(max_iter):
        changed = False
        random.shuffle(node_list)
        for n in node_list:
            neighbors = adj.get(n, set()) & nodes
            if not neighbors:
                continue
            # Count neighbor labels
            counts: dict[int, int] = {}
            for nb in neighbors:
                lbl = labels[nb]
                counts[lbl] = counts.get(lbl, 0) + 1
            # Adopt most frequent
            best = max(counts, key=counts.get)
            if labels[n] != best:
                labels[n] = best
                changed = True
        if not changed:
            break

    # Re-number clusters 0..K-1
    unique = sorted(set(labels.values()))
    remap = {old: new for new, old in enumerate(unique)}
    return {n: remap[l] for n, l in labels.items()}


def generate_session_summary(session: dict, messages: list[dict], entities: list, concepts: list) -> str:
    """Generate a markdown summary page for a session."""
    lines = [
        f"# Session: {session.get('name', 'Untitled')}",
        "",
        f"**Date:** {session.get('created_at', 'unknown')}  ",
        f"**Messages:** {len(messages)}  ",
        f"**Laureates:** {', '.join(e['name'] for e in entities)}  ",
        f"**Topics:** {', '.join(c['name'] for c in concepts)}",
        "",
        "## Key Exchanges",
        "",
    ]
    # Include notable messages (laureate responses, not too long)
    notable = [m for m in messages if m.get("role") == "laureate" and len(m.get("content", "")) > 50]
    for m in notable[:8]:
        slug = m.get("laureate_slug", "unknown")
        content = m["content"][:300]
        lines.append(f"**{slug}:** {content}{'...' if len(m['content']) > 300 else ''}")
        lines.append("")

    if concepts:
        lines.append("## Concepts Discussed")
        lines.append("")
        for c in concepts:
            lines.append(f"- **{c['name']}** ({c['count']} mentions, phase: {c['phase']})")
        lines.append("")

    return "\n".join(lines)


def generate_concept_page(name: str, timeline_entries: list[dict], existing_content: str = "") -> str:
    """Generate or update a concept wiki page with timeline."""
    lines = [
        f"# {name.title()}",
        "",
        "## Timeline",
        "",
    ]
    for entry in timeline_entries:
        phase_emoji = {"emerging": "🌱", "established": "🏛️", "challenged": "⚡",
                       "revised": "🔄", "deprecated": "📦", "mentioned": "💬"}.get(entry.get("phase", ""), "💬")
        lines.append(f"### {phase_emoji} {entry.get('created_at', '')[:10]} — {entry.get('session_name', 'Session')}")
        lines.append("")
        snapshot = entry.get("snapshot", "")
        lines.append(snapshot)
        lines.append("")

    # Preserve any existing manually-added content at the bottom
    if existing_content:
        # Extract content after "## Notes" if it exists
        notes_match = re.search(r"(## Notes.*)", existing_content, re.DOTALL)
        if notes_match:
            lines.append(notes_match.group(1))

    return "\n".join(lines)


def generate_entity_page(agent_dict: dict, session_appearances: list[dict]) -> str:
    """Generate a wiki page for a laureate entity."""
    lines = [
        f"# {agent_dict.get('name', 'Unknown')}",
        "",
        f"**Year:** {agent_dict.get('year', '?')}  ",
        f"**Achievement:** {agent_dict.get('achievement', '')}  ",
        f"**Era:** {agent_dict.get('era', '')}",
        "",
        "## Session Appearances",
        "",
    ]
    for app in session_appearances:
        lines.append(f"- **{app.get('session_name', 'Session')}** ({app.get('date', '')[:10]}): "
                      f"{app.get('context', '')[:150]}")
    lines.append("")
    return "\n".join(lines)


def generate_index(pages: list[dict]) -> str:
    """Generate the wiki index page."""
    lines = ["# Wiki Index", "", f"*{len(pages)} pages — last updated {datetime.now().strftime('%Y-%m-%d %H:%M')}*", ""]

    by_type: dict[str, list] = {}
    for p in pages:
        by_type.setdefault(p["page_type"], []).append(p)

    type_labels = {
        "session_summary": "📝 Session Summaries",
        "concept": "💡 Concepts",
        "entity": "👤 Entities",
        "synthesis": "🔗 Syntheses",
    }
    for ptype, label in type_labels.items():
        items = by_type.get(ptype, [])
        if not items:
            continue
        lines.append(f"## {label}")
        lines.append("")
        for p in sorted(items, key=lambda x: x.get("updated_at", ""), reverse=True):
            lines.append(f"- [[{p['slug']}|{p['title']}]] — v{p.get('version', 1)}, updated {str(p.get('updated_at', ''))[:10]}")
        lines.append("")

    return "\n".join(lines)


# ── Wiki Engine class ───────────────────────────────────────────

class WikiEngine:
    """Manages the wiki: ingest, query, lint, index, log."""

    def __init__(self, session_manager: SessionManager, agent_manager: AgentManager):
        self.sm = session_manager
        self.am = agent_manager
        self.conn = session_manager.conn  # share DuckDB connection

    # ── Page CRUD ──

    def get_page(self, slug: str) -> dict | None:
        r = self.conn.execute(
            "SELECT id,slug,title,page_type,content,frontmatter,content_hash,parent_hash,created_at,updated_at,version "
            "FROM wiki_pages WHERE slug=?", [slug]).fetchone()
        if not r: return None
        fm = None
        try: fm = json.loads(r[5]) if r[5] else None
        except (json.JSONDecodeError, TypeError): pass
        return {"id": r[0], "slug": r[1], "title": r[2], "page_type": r[3], "content": r[4],
                "frontmatter": fm, "content_hash": r[6], "parent_hash": r[7],
                "created_at": str(r[8]), "updated_at": str(r[9]), "version": r[10]}

    def upsert_page(self, slug: str, title: str, page_type: str, content: str,
                    frontmatter: dict | None = None) -> dict:
        """Content-addressed upsert. Skips write if hash unchanged (like git).
        Saves old version to wiki_page_history before overwriting."""
        now = datetime.now().isoformat()
        new_hash = _content_hash(content, frontmatter)
        existing = self.get_page(slug)
        fm_json = json.dumps(frontmatter) if frontmatter else None

        if existing:
            # Hash compare: skip if content unchanged
            if existing.get("content_hash") == new_hash:
                return {**existing, "_skipped": True}

            # Save old version to history before overwriting
            hid = str(uuid.uuid4())[:8]
            old_fm = json.dumps(existing["frontmatter"]) if existing.get("frontmatter") else None
            self.conn.execute(
                "INSERT INTO wiki_page_history VALUES (?,?,?,?,?,?,?,?)",
                [hid, slug, existing["version"], existing["title"],
                 existing["content"], old_fm, existing.get("content_hash", ""), now])

            # Content changed: new version, parent_hash = old hash (git-tree chain)
            new_ver = existing["version"] + 1
            parent = existing.get("content_hash", "")
            self.conn.execute(
                "UPDATE wiki_pages SET title=?,content=?,frontmatter=?,content_hash=?,parent_hash=?,updated_at=?,version=? WHERE slug=?",
                [title, content, fm_json, new_hash, parent, now, new_ver, slug])
            return {**existing, "title": title, "content": content, "frontmatter": frontmatter,
                    "content_hash": new_hash, "parent_hash": parent,
                    "updated_at": now, "version": new_ver, "_skipped": False}
        else:
            pid = str(uuid.uuid4())[:8]
            self.conn.execute(
                "INSERT INTO wiki_pages VALUES (?,?,?,?,?,?,?,?,?,?,1)",
                [pid, slug, title, page_type, content, fm_json, new_hash, None, now, now])
            return {"id": pid, "slug": slug, "title": title, "page_type": page_type,
                    "content": content, "frontmatter": frontmatter,
                    "content_hash": new_hash, "parent_hash": None,
                    "created_at": now, "updated_at": now, "version": 1, "_skipped": False}

    def delete_page(self, slug: str):
        self.conn.execute("DELETE FROM wiki_pages WHERE slug=?", [slug])
        self.conn.execute("DELETE FROM wiki_links WHERE from_slug=? OR to_slug=?", [slug, slug])

    def list_pages(self, page_type: str | None = None) -> list[dict]:
        if page_type:
            rows = self.conn.execute(
                "SELECT id,slug,title,page_type,frontmatter,content_hash,parent_hash,created_at,updated_at,version "
                "FROM wiki_pages WHERE page_type=? ORDER BY updated_at DESC", [page_type]).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT id,slug,title,page_type,frontmatter,content_hash,parent_hash,created_at,updated_at,version "
                "FROM wiki_pages ORDER BY updated_at DESC").fetchall()
        results = []
        for r in rows:
            fm = None
            try: fm = json.loads(r[4]) if r[4] else None
            except (json.JSONDecodeError, TypeError): pass
            results.append({"id": r[0], "slug": r[1], "title": r[2], "page_type": r[3],
                            "frontmatter": fm, "content_hash": r[5], "parent_hash": r[6],
                            "created_at": str(r[7]), "updated_at": str(r[8]), "version": r[9]})
        return results

    def search_pages(self, query: str, limit: int = 20) -> list[dict]:
        """Search wiki pages. Normalizes query, searches title + slug + content.
        Ranks: exact title > slug match > content match."""
        query = query.strip()
        if not query:
            return []

        q_lower = query.lower()
        q_slug = _slugify(query)
        q_like = f"%{q_lower}%"
        q_slug_like = f"%{q_slug}%"

        # Search title, slug, and content
        rows = self.conn.execute(
            "SELECT slug, title, page_type, content, updated_at FROM wiki_pages "
            "WHERE title ILIKE ? OR slug ILIKE ? OR content ILIKE ? "
            "ORDER BY updated_at DESC LIMIT ?",
            [q_like, q_slug_like, q_like, limit * 2]).fetchall()  # fetch extra, we'll re-rank

        # Score and rank
        results = []
        seen = set()
        for r in rows:
            slug, title, ptype, content, updated = r
            if slug in seen:
                continue
            seen.add(slug)
            score = 0
            tl = title.lower()
            # Exact title match (ignoring case)
            if tl == q_lower:
                score = 100
            elif q_lower in tl:
                score = 80
            elif q_slug in slug:
                score = 60
            elif q_lower in content.lower():
                score = 20
            results.append({"slug": slug, "title": title, "page_type": ptype,
                            "snippet": content[:200], "updated_at": str(updated), "_score": score})

        # Also search by individual words for multi-word queries
        words = [w for w in q_lower.split() if len(w) >= 3]
        if len(words) > 1:
            for word in words:
                wlike = f"%{word}%"
                # Use parameterized NOT IN (avoid SQL injection from seen set)
                if seen:
                    placeholders = ",".join("?" for _ in seen)
                    extra = self.conn.execute(
                        f"SELECT slug, title, page_type, content, updated_at FROM wiki_pages "
                        f"WHERE (title ILIKE ? OR slug ILIKE ?) AND slug NOT IN ({placeholders}) LIMIT 5",
                        [wlike, f"%{_slugify(word)}%"] + list(seen)).fetchall()
                else:
                    extra = self.conn.execute(
                        "SELECT slug, title, page_type, content, updated_at FROM wiki_pages "
                        "WHERE (title ILIKE ? OR slug ILIKE ?) LIMIT 5",
                        [wlike, f"%{_slugify(word)}%"]).fetchall()
                for r in extra:
                    if r[0] not in seen:
                        seen.add(r[0])
                        results.append({"slug": r[0], "title": r[1], "page_type": r[2],
                                        "snippet": r[3][:200], "updated_at": str(r[4]), "_score": 10})

        results.sort(key=lambda x: -x.get("_score", 0))
        for r in results:
            r.pop("_score", None)
        return results[:limit]

    # ── Links ──

    def add_link(self, from_slug: str, to_slug: str, link_type: str = "mentions"):
        try:
            self.conn.execute("INSERT INTO wiki_links VALUES (?,?,?,?)",
                              [from_slug, to_slug, link_type, datetime.now().isoformat()])
        except Exception:
            pass  # duplicate key — expected

    def get_links(self, slug: str) -> dict:
        outbound = self.conn.execute("SELECT to_slug,link_type FROM wiki_links WHERE from_slug=?", [slug]).fetchall()
        inbound = self.conn.execute("SELECT from_slug,link_type FROM wiki_links WHERE to_slug=?", [slug]).fetchall()
        return {
            "outbound": [{"slug": r[0], "type": r[1]} for r in outbound],
            "inbound": [{"slug": r[0], "type": r[1]} for r in inbound],
        }

    # ── Log ──

    def append_log(self, action: str, detail: str, session_id: str | None = None):
        lid = str(uuid.uuid4())[:8]
        self.conn.execute("INSERT INTO wiki_log VALUES (?,?,?,?,?)",
                          [lid, action, detail, session_id, datetime.now().isoformat()])

    def get_log(self, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id,action,detail,session_id,created_at FROM wiki_log ORDER BY created_at DESC LIMIT ?",
            [limit]).fetchall()
        return [{"id": r[0], "action": r[1], "detail": r[2], "session_id": r[3], "created_at": str(r[4])} for r in rows]

    # ── Concept Timeline ──

    def add_concept_snapshot(self, concept_slug: str, session_id: str,
                             session_name: str, snapshot: str, phase: str) -> bool:
        """Add a timeline snapshot. Returns False if identical snapshot already exists (hash dedup)."""
        snap_hash = _content_hash(snapshot, {"phase": phase, "session_id": session_id})

        # Check if we already have this exact snapshot for this concept+session
        existing = self.conn.execute(
            "SELECT id FROM wiki_concept_timeline WHERE concept_slug=? AND session_id=? AND content_hash=?",
            [concept_slug, session_id, snap_hash]).fetchone()
        if existing:
            return False  # no-op, content unchanged

        cid = str(uuid.uuid4())[:8]
        self.conn.execute("INSERT INTO wiki_concept_timeline VALUES (?,?,?,?,?,?,?,?)",
                          [cid, concept_slug, session_id, session_name, snapshot, snap_hash, phase,
                           datetime.now().isoformat()])
        return True

    def get_concept_timeline(self, concept_slug: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id,session_id,session_name,snapshot,content_hash,phase,created_at "
            "FROM wiki_concept_timeline WHERE concept_slug=? ORDER BY created_at ASC",
            [concept_slug]).fetchall()
        return [{"id": r[0], "session_id": r[1], "session_name": r[2], "snapshot": r[3],
                 "content_hash": r[4], "phase": r[5], "created_at": str(r[6])} for r in rows]

    # ── INGEST: the core operation ──

    def ingest_session(self, session_id: str, force: bool = False) -> dict:
        """Ingest a session into the wiki. Content-addressed: skips if unchanged.
        
        Uses git-like hash chain:
        - Session content hash: skip entire ingest if session messages unchanged
        - Page content hash: skip page update if generated content identical
        - Concept snapshot hash: skip duplicate timeline entries
        """
        # 1. Load session data
        sessions = self.sm.list_sessions()
        session = next((s for s in sessions if s["id"] == session_id), None)
        if not session:
            return {"error": "Session not found"}

        messages = self.sm.get_messages(session_id)
        if not messages:
            return {"error": "Session has no messages"}

        # 2. Session-level hash check: skip if messages haven't changed since last ingest
        session_hash = _session_content_hash(messages)
        if not force:
            prev = self.conn.execute(
                "SELECT content_hash FROM wiki_ingest_log WHERE session_id=?", [session_id]).fetchone()
            if prev and prev[0] == session_hash:
                return {
                    "session_id": session_id,
                    "session_name": session.get("name", ""),
                    "skipped": True,
                    "reason": "Session content unchanged since last ingest",
                    "content_hash": session_hash,
                    "pages_created": 0, "pages_updated": 0, "pages_skipped": 0,
                    "entities": 0, "concepts": 0,
                }

        # 3. Extract entities and concepts
        extracted = extract_entities_and_concepts(messages, self.am)
        entities = extracted["entities"]
        concepts = extracted["concepts"]

        pages_created = 0
        pages_updated = 0
        pages_skipped = 0
        snapshots_skipped = 0

        # 4. Create/update session summary page
        summary_slug = f"session-{session_id}"
        summary_content = generate_session_summary(session, messages, entities, concepts)
        summary_fm = {
            "session_id": session_id,
            "date": session.get("created_at", ""),
            "laureates": [e["slug"] for e in entities],
            "concepts": [c["name"] for c in concepts],
            "message_count": len(messages),
            "content_hash": session_hash,
        }
        result = self.upsert_page(summary_slug, f"Session: {session.get('name', session_id)}",
                                  "session_summary", summary_content, summary_fm)
        if result.get("_skipped"):
            pages_skipped += 1
        elif result.get("version", 1) > 1:
            pages_updated += 1
        else:
            pages_created += 1

        # 5. Create/update concept pages + timeline snapshots
        for concept in concepts:
            c_slug = f"concept-{_slugify(concept['name'])}"
            snapshot_text = f"Discussed {concept['count']} times. "
            if concept["contexts"]:
                snippet = concept["contexts"][0][:200]
                snapshot_text += f"Example: \"{snippet}...\""

            # Hash-deduped snapshot
            added = self.add_concept_snapshot(
                c_slug, session_id, session.get("name", ""),
                snapshot_text, concept["phase"])
            if not added:
                snapshots_skipped += 1

            # Build concept page from full timeline
            timeline = self.get_concept_timeline(c_slug)
            existing_page = self.get_page(c_slug)

            # Preserve LLM-generated body: only update timeline section
            if existing_page and (existing_page.get("frontmatter") or {}).get("generated_by_llm"):
                old_content = existing_page["content"]
                tl_marker = "## Timeline"
                body = old_content[:old_content.index(tl_marker)] if tl_marker in old_content else old_content.rstrip() + "\n\n"
                content = body + generate_concept_page(concept["name"], timeline, "").split("## Timeline", 1)[-1]
                content = body + "## Timeline" + content
            elif existing_page and "_(to be filled)_" not in existing_page.get("content", ""):
                # Has real content but not LLM — preserve body, append timeline
                old_content = existing_page["content"]
                tl_marker = "## Timeline"
                if tl_marker in old_content:
                    body = old_content[:old_content.index(tl_marker)]
                else:
                    body = old_content.rstrip() + "\n\n"
                tl_part = generate_concept_page(concept["name"], timeline, "")
                tl_idx = tl_part.find("## Timeline")
                content = body + (tl_part[tl_idx:] if tl_idx >= 0 else "")
            else:
                content = generate_concept_page(
                    concept["name"], timeline,
                    existing_page["content"] if existing_page else "")

            fm = {
                "first_seen": timeline[0]["created_at"] if timeline else "",
                "last_seen": timeline[-1]["created_at"] if timeline else "",
                "total_sessions": len(set(t["session_id"] for t in timeline)),
                "current_phase": concept["phase"],
                "phase_history": [t["phase"] for t in timeline],
            }
            result = self.upsert_page(c_slug, concept["name"].title(), "concept", content, fm)
            if result.get("_skipped"):
                pages_skipped += 1
            elif result.get("version", 1) > 1:
                pages_updated += 1
            else:
                pages_created += 1

            self.add_link(summary_slug, c_slug, "discusses")

        # 6. Create/update entity pages
        for entity in entities:
            e_slug = f"entity-{entity['slug']}"
            agent = self.am.get(entity["slug"])
            if not agent:
                continue

            all_summaries = self.list_pages("session_summary")
            appearances = []
            for s in all_summaries:
                fm = s.get("frontmatter") or {}
                if entity["slug"] in (fm.get("laureates") or []):
                    appearances.append({
                        "session_name": s["title"].replace("Session: ", ""),
                        "date": fm.get("date", s.get("created_at", "")),
                        "context": entity.get("context", ""),
                    })

            content = generate_entity_page(agent.to_dict(), appearances)
            fm = {
                "laureate_slug": entity["slug"],
                "year": agent.year,
                "era": agent.era,
                "session_count": len(appearances),
            }
            result = self.upsert_page(e_slug, agent.name, "entity", content, fm)
            if result.get("_skipped"):
                pages_skipped += 1
            elif result.get("version", 1) > 1:
                pages_updated += 1
            else:
                pages_created += 1

            self.add_link(summary_slug, e_slug, "features")
            for concept in concepts:
                self.add_link(e_slug, f"concept-{_slugify(concept['name'])}", "related")

        # 7. Cross-link scan: find mentions across ALL pages (connects manual pages too)
        all_pages = self.list_pages()
        title_to_slug = {}
        for p in all_pages:
            if p["slug"].startswith("_"): continue
            title_to_slug[p["title"].lower()] = p["slug"]
        for p in all_pages:
            if p["slug"].startswith("_"): continue
            page_full = self.get_page(p["slug"])
            if not page_full: continue
            cl = page_full["content"].lower()
            for tl, ts in title_to_slug.items():
                if ts != p["slug"] and len(tl) >= 3 and tl in cl:
                    self.add_link(p["slug"], ts, "mentions")

        # 8. Update index page
        index_content = generate_index(all_pages)
        self.upsert_page("_index", "Wiki Index", "index", index_content)

        # 8. Record session ingest hash (upsert)
        existing_log = self.conn.execute(
            "SELECT session_id FROM wiki_ingest_log WHERE session_id=?", [session_id]).fetchone()
        if existing_log:
            self.conn.execute(
                "UPDATE wiki_ingest_log SET content_hash=?, ingested_at=? WHERE session_id=?",
                [session_hash, datetime.now().isoformat(), session_id])
        else:
            self.conn.execute(
                "INSERT INTO wiki_ingest_log VALUES (?,?,?)",
                [session_id, session_hash, datetime.now().isoformat()])

        # 9. Append to log
        detail = (f"Ingested session '{session.get('name', session_id)}': "
                  f"{pages_created} created, {pages_updated} updated, {pages_skipped} skipped (unchanged), "
                  f"{len(entities)} entities, {len(concepts)} concepts, "
                  f"{snapshots_skipped} snapshot(s) deduped, hash={session_hash}")
        self.append_log("ingest", detail, session_id)

        return {
            "session_id": session_id,
            "session_name": session.get("name", ""),
            "skipped": False,
            "content_hash": session_hash,
            "pages_created": pages_created,
            "pages_updated": pages_updated,
            "pages_skipped": pages_skipped,
            "snapshots_skipped": snapshots_skipped,
            "entities": len(entities),
            "concepts": len(concepts),
            "entity_names": [e["name"] for e in entities],
            "concept_names": [c["name"] for c in concepts],
        }

    # ── LLM-enhanced ingest ─────────────────────────────────────

    def _build_session_transcript(self, messages: list[dict]) -> str:
        """Format session messages as a readable transcript for LLM."""
        lines = []
        for m in messages:
            role = m.get("laureate_slug", "user") if m.get("role") == "laureate" else "user"
            lines.append(f"[{role}]: {m.get('content', '')}")
        return "\n".join(lines)

    async def ingest_session_llm(self, session_id: str, llm_router, force: bool = False) -> dict:
        """Full LLM-enhanced ingest pipeline:
        1. LLM extracts entities + concepts from session
        2. Classify unclassified manual pages via LLM
        3. Create/update all extracted pages
        4. Rebuild links across ALL wiki pages
        5. Detect affected pages, regenerate context for changed ones
        Falls back to keyword ingest if LLM fails."""
        sessions = self.sm.list_sessions()
        session = next((s for s in sessions if s["id"] == session_id), None)
        if not session:
            return {"error": "Session not found"}

        messages = self.sm.get_messages(session_id)
        if not messages:
            return {"error": "Session has no messages"}

        session_hash = _session_content_hash(messages)
        if not force:
            prev = self.conn.execute(
                "SELECT content_hash FROM wiki_ingest_log WHERE session_id=?", [session_id]).fetchone()
            if prev and prev[0] == session_hash:
                return {"session_id": session_id, "skipped": True,
                        "reason": "Session content unchanged", "content_hash": session_hash,
                        "pages_created": 0, "pages_updated": 0}

        transcript = self._build_session_transcript(messages)
        max_ctx = getattr(llm_router, 'wiki_max_context', 20000)
        truncated = transcript[:max_ctx]
        if len(transcript) > max_ctx:
            truncated += "\n...(truncated)"

        # Gather existing wiki state
        all_existing = self.list_pages()
        existing_concepts = [p["title"] for p in all_existing if p["page_type"] == "concept"][:15]
        existing_entities = [p["title"] for p in all_existing if p["page_type"] == "entity"][:15]
        manual_pages = [p for p in all_existing
                        if (p.get("frontmatter") or {}).get("manually_created")
                        and "_(to be filled)_" in (self.get_page(p["slug"]) or {}).get("content", "")]

        # ── Step 1: LLM extraction (entities + concepts) ──
        prompt = (
            f"Analyze this chat session. Output ONLY valid JSON.\n\n"
            f"Session: \"{session.get('name', 'Untitled')}\"\n"
            f"Transcript:\n{truncated}\n\n"
            f"Existing concepts: {', '.join(existing_concepts) if existing_concepts else 'none'}\n"
            f"Existing entities: {', '.join(existing_entities) if existing_entities else 'none'}\n"
        )
        if manual_pages:
            prompt += f"Unclassified manual pages (need classification): {', '.join(p['title'] for p in manual_pages)}\n"
        prompt += (
            f"\nRespond with JSON:\n"
            f'{{"summary":"2-3 sentences",'
            f'"entities":[{{"name":"...","type":"person|tool|system|organization","description":"one sentence"}}],'
            f'"concepts":[{{"name":"...","definition":"one sentence","phase":"emerging|established|challenged|revised|mentioned","notes":"key insight"}}],'
            f'"cross_references":[{{"from":"name","to":"name","relationship":"..."}}]'
        )
        if manual_pages:
            prompt += f',"classifications":[{{"page_title":"...","should_be":"entity|concept","reason":"..."}}]'
        prompt += '}'

        try:
            raw = await llm_router.chat_complete_wiki(
                messages=[{"role": "user", "content": prompt}],
                system="Output only valid JSON. No markdown fences. Be concise."
            )
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw.rsplit("```", 1)[0]
            raw = raw.strip()
            if not raw:
                raise ValueError("Empty LLM response")
            llm_data = json.loads(raw)
        except json.JSONDecodeError as e:
            self.append_log("ingest_llm_fallback", f"Invalid JSON: {str(e)[:100]}", session_id)
            return self.ingest_session(session_id, force=True)
        except Exception as e:
            self.append_log("ingest_llm_fallback", f"{type(e).__name__}: {str(e)[:100]}", session_id)
            return self.ingest_session(session_id, force=True)

        pages_created = 0
        pages_updated = 0
        pages_regenerated = 0
        changed_slugs = set()  # Track which pages changed (for step 5)

        # ── Step 2: Classify manual pages ──
        for cls in llm_data.get("classifications", []):
            title = cls.get("page_title", "")
            should_be = cls.get("should_be", "concept")
            mp = next((p for p in manual_pages if p["title"].lower() == title.lower()), None)
            if mp and mp["page_type"] != should_be:
                # Reclassify: update page_type, re-slug
                old_slug = mp["slug"]
                new_slug = f"{should_be}-{_slugify(title)}"
                if old_slug != new_slug and not self.get_page(new_slug):
                    page = self.get_page(old_slug)
                    if page:
                        self.upsert_page(new_slug, page["title"], should_be, page["content"],
                                         {**(page.get("frontmatter") or {}), "reclassified_from": old_slug})
                        # Move links
                        self.conn.execute("DELETE FROM wiki_links WHERE from_slug=? OR to_slug=?", [old_slug, old_slug])
                        self.delete_page(old_slug)
                        changed_slugs.add(new_slug)
                        self.append_log("reclassify", f"'{title}' reclassified from {mp['page_type']} to {should_be}")

        # ── Step 3a: Session summary page ──
        summary_slug = f"session-{session_id}"
        summary_content = f"# Session: {session.get('name', session_id)}\n\n"
        summary_content += f"**Date:** {session.get('created_at', '')}  \n"
        summary_content += f"**Messages:** {len(messages)}\n\n"
        summary_content += f"## Summary\n\n{llm_data.get('summary', '')}\n\n"

        llm_entities = llm_data.get("entities", [])
        llm_concepts = llm_data.get("concepts", [])

        if llm_entities:
            summary_content += "## Entities\n\n"
            for e in llm_entities:
                summary_content += f"- **{e.get('name', '?')}** ({e.get('type', '?')}) — {e.get('description', '')}\n"
        if llm_concepts:
            summary_content += "\n## Concepts\n\n"
            for c in llm_concepts:
                summary_content += f"- **{c.get('name', '?')}** — {c.get('definition', '')} (phase: {c.get('phase', 'mentioned')})\n"

        summary_fm = {
            "session_id": session_id, "date": session.get("created_at", ""),
            "message_count": len(messages), "content_hash": session_hash,
            "llm_generated": True,
            "entity_names": [e.get("name", "") for e in llm_entities],
            "concept_names": [c.get("name", "") for c in llm_concepts],
        }
        result = self.upsert_page(summary_slug, f"Session: {session.get('name', session_id)}",
                                  "session_summary", summary_content, summary_fm)
        if not result.get("_skipped"):
            changed_slugs.add(summary_slug)
            if result.get("version", 1) == 1: pages_created += 1
            else: pages_updated += 1

        # ── Step 3b: Entity pages (LLM-extracted) ──
        for ent in llm_entities:
            name = ent.get("name", "").strip()
            if not name: continue
            e_slug = f"entity-{_slugify(name)}"
            etype = ent.get("type", "unknown")
            desc = ent.get("description", "")

            existing = self.get_page(e_slug)
            content = f"# {name}\n\n"
            content += f"**Type:** {etype}  \n"
            content += f"**Description:** {desc}\n\n"
            content += "## Session Appearances\n\n"
            content += f"- **{session.get('name', session_id)}** ({session.get('created_at', '')[:10]})\n"
            if existing and "## Session Appearances" in existing["content"]:
                # Append to existing appearances
                old_appearances = existing["content"].split("## Session Appearances")[1]
                content += old_appearances.strip() + "\n"

            fm = {"entity_type": etype, "llm_generated": True}
            result = self.upsert_page(e_slug, name, "entity", content, fm)
            if not result.get("_skipped"):
                changed_slugs.add(e_slug)
                if result.get("version", 1) == 1: pages_created += 1
                else: pages_updated += 1
            self.add_link(summary_slug, e_slug, "features")

        # ── Step 3c: Concept pages ──
        for concept in llm_concepts:
            name = concept.get("name", "").strip()
            if not name: continue
            c_slug = f"concept-{_slugify(name)}"
            phase = concept.get("phase", "mentioned")
            definition = concept.get("definition", "")
            notes = concept.get("notes", "")

            snapshot_text = f"{definition} {notes}".strip()
            self.add_concept_snapshot(c_slug, session_id, session.get("name", ""), snapshot_text, phase)

            timeline = self.get_concept_timeline(c_slug)
            existing_page = self.get_page(c_slug)

            # Incremental update: if page already has LLM-generated content, only update
            # timeline + metadata. Don't rewrite the body sections.
            if existing_page and (existing_page.get("frontmatter") or {}).get("generated_by_llm"):
                # Preserve existing body, rebuild only timeline section
                old_content = existing_page["content"]
                tl_marker = "## Timeline"
                if tl_marker in old_content:
                    body_before_tl = old_content[:old_content.index(tl_marker)]
                else:
                    body_before_tl = old_content.rstrip() + "\n\n"

                tl_content = f"{tl_marker}\n\n"
                for entry in timeline:
                    pe = {"emerging": "🌱", "established": "🏛️", "challenged": "⚡",
                          "revised": "🔄", "deprecated": "📦", "mentioned": "💬"}.get(entry.get("phase", ""), "💬")
                    tl_content += f"### {pe} {entry.get('created_at', '')[:10]} — {entry.get('session_name', 'Session')}\n\n{entry.get('snapshot', '')}\n\n"
                content = body_before_tl + tl_content
            elif existing_page and "_(to be filled)_" not in existing_page.get("content", ""):
                # Page exists with real content but not LLM-generated — append timeline only
                old_content = existing_page["content"]
                tl_marker = "## Timeline"
                if tl_marker in old_content:
                    body_before_tl = old_content[:old_content.index(tl_marker)]
                else:
                    body_before_tl = old_content.rstrip() + "\n\n"
                tl_content = f"{tl_marker}\n\n"
                for entry in timeline:
                    pe = {"emerging": "🌱", "established": "🏛️", "challenged": "⚡",
                          "revised": "🔄", "deprecated": "📦", "mentioned": "💬"}.get(entry.get("phase", ""), "💬")
                    tl_content += f"### {pe} {entry.get('created_at', '')[:10]} — {entry.get('session_name', 'Session')}\n\n{entry.get('snapshot', '')}\n\n"
                content = body_before_tl + tl_content
            else:
                # New page or placeholder — build from scratch
                content = f"# {name}\n\n## Definition\n\n{definition}\n\n"
                if notes:
                    content += f"## Notes\n\n{notes}\n\n"
                content += "## Timeline\n\n"
                for entry in timeline:
                    pe = {"emerging": "🌱", "established": "🏛️", "challenged": "⚡",
                          "revised": "🔄", "deprecated": "📦", "mentioned": "💬"}.get(entry.get("phase", ""), "💬")
                    content += f"### {pe} {entry.get('created_at', '')[:10]} — {entry.get('session_name', 'Session')}\n\n{entry.get('snapshot', '')}\n\n"

            fm = {
                "first_seen": timeline[0]["created_at"] if timeline else "",
                "last_seen": timeline[-1]["created_at"] if timeline else "",
                "total_sessions": len(set(t["session_id"] for t in timeline)),
                "current_phase": phase,
                "phase_history": [t["phase"] for t in timeline],
                "llm_generated": True,
            }
            result = self.upsert_page(c_slug, name, "concept", content, fm)
            if not result.get("_skipped"):
                changed_slugs.add(c_slug)
                if result.get("version", 1) == 1: pages_created += 1
                else: pages_updated += 1
            self.add_link(summary_slug, c_slug, "discusses")

        # ── Step 3d: Cross-references ──
        for xref in llm_data.get("cross_references", []):
            # Try both concept- and entity- prefixes
            from_name = xref.get("from", "")
            to_name = xref.get("to", "")
            from_slug = self._resolve_slug(from_name)
            to_slug = self._resolve_slug(to_name)
            if from_slug and to_slug and from_slug != to_slug:
                self.add_link(from_slug, to_slug, xref.get("relationship", "related"))

        # Also run keyword entity extraction for laureates
        extracted = extract_entities_and_concepts(messages, self.am)
        for entity in extracted["entities"]:
            e_slug = f"entity-{entity['slug']}"
            agent = self.am.get(entity["slug"])
            if not agent: continue
            all_sums = self.list_pages("session_summary")
            apps = [{"session_name": s["title"].replace("Session: ", ""),
                     "date": (s.get("frontmatter") or {}).get("date", s.get("created_at", "")),
                     "context": entity.get("context", "")}
                    for s in all_sums if entity["slug"] in ((s.get("frontmatter") or {}).get("laureates") or [])]
            content = generate_entity_page(agent.to_dict(), apps)
            fm = {"laureate_slug": entity["slug"], "year": agent.year, "era": agent.era, "session_count": len(apps)}
            result = self.upsert_page(e_slug, agent.name, "entity", content, fm)
            if not result.get("_skipped"):
                changed_slugs.add(e_slug)
                if result.get("version", 1) == 1: pages_created += 1
                else: pages_updated += 1
            self.add_link(summary_slug, e_slug, "features")

        # ── Step 4: Rebuild links for ALL pages ──
        all_pages_now = self.list_pages()
        title_to_slug = {}
        for p in all_pages_now:
            if p["slug"].startswith("_"): continue
            title_to_slug[p["title"].lower()] = p["slug"]
            # Also map slug keywords
            short = p["slug"].replace("concept-", "").replace("entity-", "").replace("session-", "").replace("-", " ")
            if len(short) > 3:
                title_to_slug[short] = p["slug"]

        for p in all_pages_now:
            if p["slug"].startswith("_"): continue
            page_full = self.get_page(p["slug"])
            if not page_full: continue
            content_lower = page_full["content"].lower()
            for title_lower, target_slug in title_to_slug.items():
                if target_slug == p["slug"]: continue
                if title_lower in content_lower:
                    self.add_link(p["slug"], target_slug, "mentions")

        # ── Step 5: Regenerate affected pages ──
        # Find neighbors of changed pages that have stale content
        affected_slugs = set()
        for slug in changed_slugs:
            links = self.get_links(slug)
            for l in links.get("inbound", []):
                if l["slug"] not in changed_slugs:
                    affected_slugs.add(l["slug"])
            for l in links.get("outbound", []):
                if l["slug"] not in changed_slugs:
                    affected_slugs.add(l["slug"])

        # Regenerate concept pages that are affected (have placeholder content or stale context)
        for slug in affected_slugs:
            page = self.get_page(slug)
            if not page or page["page_type"] != "concept": continue
            fm = page.get("frontmatter") or {}
            # Skip if already LLM-generated in this session
            if slug in changed_slugs: continue
            # Only regenerate if it has placeholder content or its neighbors changed
            has_placeholder = "_(to be filled)_" in page.get("content", "")
            if has_placeholder:
                try:
                    result = await self.generate_structured_page(slug, llm_router)
                    if result.get("generated"):
                        pages_regenerated += 1
                        self.append_log("regen", f"Regenerated '{page['title']}' (affected by ingest)")
                except Exception:
                    pass  # Non-critical, don't block ingest

        # ── Step 6: Update index + log ──
        index_content = generate_index(self.list_pages())
        self.upsert_page("_index", "Wiki Index", "index", index_content)

        existing_log = self.conn.execute(
            "SELECT session_id FROM wiki_ingest_log WHERE session_id=?", [session_id]).fetchone()
        if existing_log:
            self.conn.execute("UPDATE wiki_ingest_log SET content_hash=?, ingested_at=? WHERE session_id=?",
                              [session_hash, datetime.now().isoformat(), session_id])
        else:
            self.conn.execute("INSERT INTO wiki_ingest_log VALUES (?,?,?)",
                              [session_id, session_hash, datetime.now().isoformat()])

        detail = (f"LLM-ingested '{session.get('name', session_id)}': "
                  f"{pages_created} created, {pages_updated} updated, {pages_regenerated} regenerated, "
                  f"{len(llm_entities)} entities, {len(llm_concepts)} concepts, "
                  f"{len(changed_slugs)} changed, {len(affected_slugs)} affected")
        self.append_log("ingest_llm", detail, session_id)

        return {
            "session_id": session_id,
            "session_name": session.get("name", ""),
            "skipped": False, "content_hash": session_hash,
            "pages_created": pages_created, "pages_updated": pages_updated,
            "pages_regenerated": pages_regenerated,
            "entities": len(llm_entities), "concepts": len(llm_concepts),
            "entity_names": [e.get("name", "") for e in llm_entities],
            "concept_names": [c.get("name", "") for c in llm_concepts],
            "changed": len(changed_slugs), "affected": len(affected_slugs),
            "llm_enhanced": True,
        }

    def _resolve_slug(self, name: str) -> str | None:
        """Resolve a name to an existing wiki page slug. Tries concept- then entity- prefix."""
        if not name: return None
        slug = f"concept-{_slugify(name)}"
        if self.get_page(slug): return slug
        slug = f"entity-{_slugify(name)}"
        if self.get_page(slug): return slug
        # Try direct match
        rows = self.conn.execute(
            "SELECT slug FROM wiki_pages WHERE title ILIKE ? LIMIT 1", [name]).fetchone()
        return rows[0] if rows else None

    # ── Manual page creation ────────────────────────────────────

    def create_manual_page(self, title: str, page_type: str = "concept") -> dict:
        """Create a new wiki page manually with template sections.
        Auto-links to existing pages that mention this concept."""
        slug = f"{page_type}-{_slugify(title)}" if page_type != "manual" else f"page-{_slugify(title)}"

        existing = self.get_page(slug)
        if existing:
            return {"error": f"Page '{slug}' already exists", "slug": slug}

        sections = self.TEMPLATES.get(page_type, {}).get("sections", [])
        content = f"# {title}\n\n"
        for section in sections:
            content += f"## {section}\n\n_(to be filled)_\n\n"
        if not sections:
            content += "_(add content here)_\n"

        fm = {"manually_created": True, "page_type": page_type}
        self.upsert_page(slug, title, page_type, content, fm)

        # Auto-link: find existing pages whose content mentions this title
        title_lower = title.lower()
        links_created = 0
        for p in self.list_pages():
            if p["slug"] == slug or p["slug"].startswith("_"):
                continue
            other = self.get_page(p["slug"])
            if other and title_lower in other.get("content", "").lower():
                self.add_link(p["slug"], slug, "mentions")
                links_created += 1

        # Update index
        index_content = generate_index(self.list_pages())
        self.upsert_page("_index", "Wiki Index", "index", index_content)

        self.append_log("manual_create", f"Created manual page '{title}' ({page_type}), {links_created} auto-links")
        return {"slug": slug, "title": title, "page_type": page_type, "version": 1, "links_created": links_created}

    # ── LINT: health check ──

    def lint(self) -> dict:
        """Health-check the wiki. Returns issues found."""
        issues = []
        pages = self.list_pages()
        slugs = {p["slug"] for p in pages}

        # Orphan pages (no inbound links, excluding index)
        for p in pages:
            if p["slug"].startswith("_"):
                continue
            links = self.get_links(p["slug"])
            if not links["inbound"] and p["page_type"] != "index":
                issues.append({"type": "orphan", "slug": p["slug"], "title": p["title"]})

        # Broken links (link to non-existent page)
        all_links = self.conn.execute("SELECT from_slug, to_slug, link_type FROM wiki_links").fetchall()
        for from_s, to_s, lt in all_links:
            if to_s not in slugs:
                issues.append({"type": "broken_link", "from": from_s, "to": to_s})

        # Stale concept pages (not updated in any recent session)
        concept_pages = [p for p in pages if p["page_type"] == "concept"]
        for cp in concept_pages:
            fm = cp.get("frontmatter") or {}
            if fm.get("total_sessions", 0) == 1:
                issues.append({"type": "single_source", "slug": cp["slug"], "title": cp["title"],
                               "suggestion": "This concept only appears in one session"})

        # Concept phase drift detection
        for cp in concept_pages:
            fm = cp.get("frontmatter") or {}
            phases = fm.get("phase_history", [])
            if len(phases) >= 2 and phases[-1] != phases[-2]:
                issues.append({"type": "phase_shift", "slug": cp["slug"], "title": cp["title"],
                               "from_phase": phases[-2], "to_phase": phases[-1],
                               "suggestion": f"Concept shifted from '{phases[-2]}' to '{phases[-1]}'"})

        # Contradiction detection (R3.9)
        contradictions = self.detect_contradictions()
        for c in contradictions:
            issues.append({
                "type": "contradiction",
                "slug": c["slug"],
                "title": c["concept"],
                "suggestion": c["suggestion"],
                "detail": c,
            })
        self.append_log("lint", f"Found {len(issues)} issues")
        return {"issues": issues, "total_pages": len(pages), "total_links": len(all_links)}

    # ── Stats ──

    def stats(self) -> dict:
        page_count = self.conn.execute("SELECT COUNT(*) FROM wiki_pages").fetchone()[0]
        link_count = self.conn.execute("SELECT COUNT(*) FROM wiki_links").fetchone()[0]
        log_count = self.conn.execute("SELECT COUNT(*) FROM wiki_log").fetchone()[0]
        timeline_count = self.conn.execute("SELECT COUNT(*) FROM wiki_concept_timeline").fetchone()[0]
        by_type = {}
        for r in self.conn.execute("SELECT page_type, COUNT(*) FROM wiki_pages GROUP BY page_type").fetchall():
            by_type[r[0]] = r[1]
        return {"pages": page_count, "links": link_count, "log_entries": log_count,
                "timeline_entries": timeline_count, "by_type": by_type}

    # ── Graph data for d3.js visualization ──

    def graph_data(self, focus: str | None = None, hops: int = 1) -> dict:
        """Return nodes + edges with cluster IDs (label propagation).
        If focus set, returns N-hop subgraph around that node."""
        pages = self.conn.execute(
            "SELECT slug, title, page_type, version FROM wiki_pages WHERE slug != '_index'"
        ).fetchall()
        links = self.conn.execute(
            "SELECT from_slug, to_slug, link_type FROM wiki_links"
        ).fetchall()

        all_slugs = {r[0] for r in pages}
        page_map = {r[0]: {"id": r[0], "label": r[1], "type": r[2], "version": r[3]} for r in pages}

        # Adjacency + degree
        adj: dict[str, set[str]] = {s: set() for s in all_slugs}
        degree: dict[str, int] = {}
        edge_list = []
        for from_s, to_s, lt in links:
            if from_s in all_slugs and to_s in all_slugs:
                edge_list.append({"source": from_s, "target": to_s, "type": lt})
                adj[from_s].add(to_s)
                adj[to_s].add(from_s)
                degree[from_s] = degree.get(from_s, 0) + 1
                degree[to_s] = degree.get(to_s, 0) + 1

        # Subgraph: BFS from focus (R3.2)
        visible = all_slugs
        if focus and focus in all_slugs:
            visible = {focus}
            frontier = {focus}
            for _ in range(hops):
                nxt = set()
                for n in frontier:
                    nxt |= adj.get(n, set())
                visible |= nxt
                frontier = nxt
            edge_list = [e for e in edge_list if e["source"] in visible and e["target"] in visible]

        # Clustering: label propagation (R3.1)
        clusters = _label_propagation(visible, adj)

        nodes = []
        for slug in visible:
            if slug not in page_map:
                continue
            n = {**page_map[slug]}
            n["degree"] = degree.get(slug, 0)
            n["cluster"] = clusters.get(slug, 0)
            nodes.append(n)

        return {"nodes": nodes, "edges": edge_list, "focus": focus, "hops": hops}

    # ── Page History & Diff (R3.7) ──

    def get_page_history(self, slug: str) -> list[dict]:
        """Get all saved versions of a page (oldest first)."""
        rows = self.conn.execute(
            "SELECT id,slug,version,title,content,frontmatter,content_hash,saved_at "
            "FROM wiki_page_history WHERE slug=? ORDER BY version ASC", [slug]).fetchall()
        results = []
        for r in rows:
            fm = None
            try: fm = json.loads(r[5]) if r[5] else None
            except (json.JSONDecodeError, TypeError): pass
            results.append({"id": r[0], "slug": r[1], "version": r[2], "title": r[3],
                            "content": r[4], "frontmatter": fm,
                            "content_hash": r[6], "saved_at": str(r[7])})
        # Append current version
        current = self.get_page(slug)
        if current:
            results.append({
                "id": current["id"], "slug": slug, "version": current["version"],
                "title": current["title"], "content": current["content"],
                "frontmatter": current["frontmatter"], "content_hash": current["content_hash"],
                "saved_at": current["updated_at"],
            })
        return results

    def diff_pages(self, slug: str, v1: int, v2: int) -> dict:
        """Line-based diff between two versions of a page.
        Returns {v1, v2, lines: [{op: 'equal'|'add'|'remove', text}]}."""
        history = self.get_page_history(slug)
        ver_map = {h["version"]: h for h in history}
        if v1 not in ver_map or v2 not in ver_map:
            return {"error": f"Version {v1} or {v2} not found for {slug}"}

        lines_a = ver_map[v1]["content"].splitlines()
        lines_b = ver_map[v2]["content"].splitlines()

        # Simple LCS-based diff (no external deps)
        diff_lines = _simple_diff(lines_a, lines_b)
        return {
            "slug": slug,
            "v1": v1, "v2": v2,
            "title_v1": ver_map[v1]["title"],
            "title_v2": ver_map[v2]["title"],
            "hash_v1": ver_map[v1]["content_hash"],
            "hash_v2": ver_map[v2]["content_hash"],
            "lines": diff_lines,
            "stats": {
                "added": sum(1 for l in diff_lines if l["op"] == "add"),
                "removed": sum(1 for l in diff_lines if l["op"] == "remove"),
                "equal": sum(1 for l in diff_lines if l["op"] == "equal"),
            }
        }

    # ── Contradiction Detection (R3.9) ──

    def detect_contradictions(self) -> list[dict]:
        """Find concepts with opposing claims across sessions."""
        contradictions = []
        concept_pages = self.list_pages("concept")

        # Phase-based: concept is "established" in one session, "challenged" in another
        for cp in concept_pages:
            fm = cp.get("frontmatter") or {}
            phases = fm.get("phase_history", [])
            if len(phases) < 2:
                continue

            # Check for opposing phase pairs
            phase_set = set(phases)
            opposing = [
                ("established", "challenged"),
                ("established", "deprecated"),
                ("emerging", "deprecated"),
                ("established", "revised"),
            ]
            for p_a, p_b in opposing:
                if p_a in phase_set and p_b in phase_set:
                    # Find the sessions where each phase occurred
                    timeline = self.get_concept_timeline(cp["slug"])
                    sessions_a = [t["session_name"] for t in timeline if t["phase"] == p_a]
                    sessions_b = [t["session_name"] for t in timeline if t["phase"] == p_b]
                    contradictions.append({
                        "type": "phase_contradiction",
                        "concept": cp["title"],
                        "slug": cp["slug"],
                        "phase_a": p_a,
                        "phase_b": p_b,
                        "sessions_a": sessions_a[:3],
                        "sessions_b": sessions_b[:3],
                        "suggestion": f"'{cp['title']}' was '{p_a}' in {sessions_a[0] if sessions_a else '?'} but '{p_b}' in {sessions_b[0] if sessions_b else '?'}",
                    })

        # Keyword-based: same concept has both positive and negative sentiment keywords
        positive_kw = {"correct", "proven", "effective", "works", "best", "superior", "successful"}
        negative_kw = {"wrong", "flawed", "failed", "inferior", "broken", "obsolete", "disproven"}
        for cp in concept_pages:
            timeline = self.get_concept_timeline(cp["slug"])
            if len(timeline) < 2:
                continue
            for i, t1 in enumerate(timeline):
                s1 = t1["snapshot"].lower()
                has_pos = any(kw in s1 for kw in positive_kw)
                for t2 in timeline[i+1:]:
                    if t2["session_id"] == t1["session_id"]:
                        continue
                    s2 = t2["snapshot"].lower()
                    has_neg = any(kw in s2 for kw in negative_kw)
                    if has_pos and has_neg:
                        contradictions.append({
                            "type": "sentiment_contradiction",
                            "concept": cp["title"],
                            "slug": cp["slug"],
                            "session_a": t1["session_name"],
                            "session_b": t2["session_name"],
                            "snippet_a": t1["snapshot"][:120],
                            "snippet_b": t2["snapshot"][:120],
                            "suggestion": f"Opposing views on '{cp['title']}' between sessions",
                        })
                        break  # one per concept is enough
                if any(c["slug"] == cp["slug"] and c["type"] == "sentiment_contradiction" for c in contradictions):
                    break

        return contradictions

    # ── Wiki Templates (R3.10) ──────────────────────────────────

    TEMPLATES = {
        "concept": {
            "sections": ["Definition", "Introduction", "Synonyms", "Use Scenarios", "Examples", "Timeline", "See Also"],
            "prompt": (
                "Write a structured wiki page about the concept \"{title}\".\n"
                "Context from chat sessions:\n{context}\n\n"
                "Produce ONLY markdown with these sections:\n"
                "## Definition\nOne clear sentence defining the concept.\n"
                "## Introduction\n2-3 sentence overview.\n"
                "## Synonyms\nRelated terms or alternative names.\n"
                "## Use Scenarios\nWhere and how this concept applies.\n"
                "## Examples\nConcrete examples from the context above.\n\n"
                "Be concise. No preamble."
            ),
        },
        "disambiguation": {
            "sections": ["Overview", "Meanings"],
            "prompt": (
                "Write a disambiguation wiki page for \"{title}\".\n"
                "Context:\n{context}\n\n"
                "List the different meanings or uses of this term. Be concise."
            ),
        },
        "entity": {
            "sections": ["Biography", "Key Contributions", "Thinking Style", "Session Appearances"],
            "prompt": (
                "Write a structured wiki page about the entity \"{title}\".\n"
                "Context from chat sessions:\n{context}\n\n"
                "Produce ONLY markdown with these sections:\n"
                "## Overview\nWho or what this is, in 1-2 sentences.\n"
                "## Key Contributions\nMajor achievements or significance.\n"
                "## Connections\nRelated concepts, people, or systems.\n\n"
                "Be concise. No preamble."
            ),
        },
    }

    def get_template_sections(self, page_type: str) -> list[str]:
        """Return the template section names for a page type."""
        t = self.TEMPLATES.get(page_type)
        return t["sections"] if t else []

    # ── Consolidation ────────────────────────────────────────────

    def find_related_concepts(self, min_similarity: int = 1, ignore: list[str] | None = None) -> list[dict]:
        """Find concept pages that might be duplicates or closely related.
        Groups by shared words in slug/title. ignore: list of 'slugA|slugB' pairs to skip."""
        ignore_set = set(ignore or [])
        concepts = self.list_pages("concept")
        if len(concepts) < 2:
            return []

        # Build word sets per concept
        word_map: dict[str, set[str]] = {}
        for c in concepts:
            words = set(c["slug"].replace("concept-", "").split("-"))
            words |= set(c["title"].lower().split())
            words -= {"", "the", "a", "an", "and", "or", "of", "in", "for", "to", "on"}
            word_map[c["slug"]] = words

        # Find groups with shared words
        groups: list[dict] = []
        seen: set[str] = set()
        slugs = [c["slug"] for c in concepts]
        concept_by_slug = {c["slug"]: c for c in concepts}

        for i, s1 in enumerate(slugs):
            if s1 in seen:
                continue
            group = [s1]
            for s2 in slugs[i + 1:]:
                if s2 in seen:
                    continue
                # Check ignore list (both orderings)
                pair_key = f"{s1}|{s2}" if s1 < s2 else f"{s2}|{s1}"
                if pair_key in ignore_set:
                    continue
                shared = word_map[s1] & word_map[s2]
                if len(shared) >= min_similarity:
                    group.append(s2)
            if len(group) >= 2:
                seen.update(group)
                groups.append({
                    "concepts": [{"slug": s, "title": concept_by_slug[s]["title"],
                                  "version": concept_by_slug[s]["version"]}
                                 for s in group],
                    "shared_words": list(word_map[group[0]] & word_map[group[1]]) if len(group) >= 2 else [],
                })

        return groups

    def consolidate_concepts(self, primary_slug: str, merge_slugs: list[str]) -> dict:
        """Merge related concept pages into the primary page.
        Combines timelines and links. Returns the updated page."""
        primary = self.get_page(primary_slug)
        if not primary:
            return {"error": f"Primary page '{primary_slug}' not found"}

        merged_timelines = []

        for slug in merge_slugs:
            page = self.get_page(slug)
            if not page:
                continue

            # Move timeline entries
            timeline = self.get_concept_timeline(slug)
            for t in timeline:
                self.conn.execute(
                    "UPDATE wiki_concept_timeline SET concept_slug=? WHERE id=?",
                    [primary_slug, t["id"]])
                merged_timelines.append(t)

            # Re-point links: delete old, re-add to primary (add_link handles duplicates)
            inbound = self.conn.execute(
                "SELECT from_slug, link_type FROM wiki_links WHERE to_slug=?", [slug]).fetchall()
            outbound = self.conn.execute(
                "SELECT to_slug, link_type FROM wiki_links WHERE from_slug=?", [slug]).fetchall()

            # Delete all links involving the merged slug
            self.conn.execute("DELETE FROM wiki_links WHERE from_slug=? OR to_slug=?", [slug, slug])

            # Re-add as links to/from primary (skip self-links)
            for from_s, lt in inbound:
                if from_s != primary_slug:
                    self.add_link(from_s, primary_slug, lt)
            for to_s, lt in outbound:
                if to_s != primary_slug:
                    self.add_link(primary_slug, to_s, lt)

            # Delete the merged page
            self.delete_page(slug)

            # Delete the merged page
            self.delete_page(slug)

        # Rebuild the primary concept page with full timeline
        all_timeline = self.get_concept_timeline(primary_slug)
        from core.wiki_engine import generate_concept_page
        content = generate_concept_page(
            primary["title"], all_timeline,
            primary["content"])
        fm = primary.get("frontmatter") or {}
        fm["total_sessions"] = len(set(t["session_id"] for t in all_timeline))
        fm["merged_from"] = merge_slugs

        self.upsert_page(primary_slug, primary["title"], "concept", content, fm)
        self.append_log("consolidate",
                        f"Merged {merge_slugs} into {primary_slug}. {len(merged_timelines)} timeline entries moved.")

        return {
            "primary": primary_slug,
            "merged": merge_slugs,
            "timelines_moved": len(merged_timelines),
        }

    def build_concept_context(self, slug: str) -> str:
        """Gather all context for a concept. Three sources (in order):
        1. Timeline snapshots (from previous ingests)
        2. Cross-session message search (keyword match in ALL messages)
        3. Existing wiki page content
        """
        parts = []

        # Source 1: Timeline snapshots
        timeline = self.get_concept_timeline(slug)
        for t in timeline:
            parts.append(f"[{t['session_name']}, {t['created_at'][:10]}, phase: {t['phase']}]\n{t['snapshot']}")

        # Source 2: Cross-session search — find messages mentioning this concept
        page = self.get_page(slug)
        keywords = []
        if page:
            # Extract search terms from title
            title_words = page["title"].lower().split()
            keywords = [w for w in title_words if len(w) > 3]
        if not keywords:
            keywords = [slug.replace("concept-", "").replace("-", " ")]

        if not parts or len(parts) < 2:
            # Search across all sessions for relevant messages
            for kw in keywords[:2]:
                rows = self.conn.execute(
                    "SELECT m.content, m.laureate_slug, s.name "
                    "FROM messages m JOIN sessions s ON m.session_id = s.id "
                    "WHERE m.content ILIKE ? ORDER BY m.created_at DESC LIMIT 10",
                    [f"%{kw}%"]).fetchall()
                for content, laureate, sess_name in rows:
                    snippet = content[:300]
                    speaker = laureate or "user"
                    parts.append(f"[{sess_name}, {speaker}]: {snippet}")

        # Source 3: Existing page content (if not just a template)
        if page and "_(to be filled)_" not in page.get("content", ""):
            existing = page["content"][:500]
            if existing.strip():
                parts.append(f"[existing wiki page]\n{existing}")

        return "\n\n".join(parts) if parts else "(no context found — try ingesting sessions first)"

    async def generate_structured_page(self, slug: str, llm_router, force: bool = False) -> dict:
        """Use LLM to generate or update a wiki page.
        If force=True, always does a full rewrite regardless of existing content.
        Otherwise, asks LLM to evaluate and merge (may return NO_CHANGE)."""
        page = self.get_page(slug)
        if not page:
            return {"error": f"Page '{slug}' not found"}

        template = self.TEMPLATES.get(page["page_type"])
        if not template or not template.get("prompt"):
            return {"error": f"No LLM template for page type '{page['page_type']}'"}

        context = self.build_concept_context(slug)
        if "(no context found" in context:
            return {"error": "No context available. Ingest some sessions first, or chat about this topic."}

        max_ctx = getattr(llm_router, 'wiki_max_context', 20000)
        if len(context) > max_ctx:
            context = context[:max_ctx] + "\n...(truncated)"

        existing_content = page.get("content", "")
        has_real_content = (existing_content
                           and "_(to be filled)_" not in existing_content
                           and len(existing_content) > 100)

        if has_real_content and not force:
            # Incremental mode: ask LLM to evaluate and merge, not rewrite
            # Strip timeline section from existing content for the prompt
            tl_idx = existing_content.find("## Timeline")
            body_to_send = existing_content[:tl_idx].strip() if tl_idx > 0 else existing_content[:1500]

            prompt = (
                f"You are updating a wiki page about \"{page['title']}\".\n\n"
                f"EXISTING PAGE:\n{body_to_send}\n\n"
                f"NEW CONTEXT (from recent sessions):\n{context}\n\n"
                f"Instructions:\n"
                f"- If the existing page is already accurate and complete, output ONLY the word: NO_CHANGE\n"
                f"- If updates are needed, output the FULL updated page in markdown.\n"
                f"- Preserve existing sections that are still accurate.\n"
                f"- Add new information from the context.\n"
                f"- Fix any inaccuracies based on the new context.\n"
                f"- Keep it under 500 words. No preamble."
            )
        else:
            # Full generation for new/placeholder pages
            prompt = template["prompt"].format(title=page["title"], context=context)

        try:
            generated = await llm_router.chat_complete_wiki(
                messages=[{"role": "user", "content": prompt}],
                system="You are a concise wiki editor. Output markdown only. Keep it under 500 words."
            )
        except Exception as e:
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg:
                return {"error": "LLM authentication failed. Check your API key in config.toml."}
            if "timeout" in err_msg.lower() or "closed" in err_msg.lower() or "incomplete" in err_msg.lower():
                return {"error": "LLM connection timed out. Try again or check your network/provider."}
            return {"error": f"LLM call failed: {err_msg[:200]}"}

        if not generated or not generated.strip():
            return {"error": "LLM returned empty response. Try again."}

        # Check if LLM said no change needed
        if generated.strip().upper() == "NO_CHANGE":
            self.append_log("generate", f"LLM evaluated '{page['title']}' — no changes needed")
            return {"slug": slug, "title": page["title"], "generated": False, "no_change": True}

        # Append existing timeline section
        timeline = self.get_concept_timeline(slug)
        if timeline:
            generated += "\n\n## Timeline\n\n"
            tl_page = generate_concept_page(page["title"], timeline, "")
            tl_start = tl_page.find("## Timeline")
            if tl_start >= 0:
                generated += tl_page[tl_start + len("## Timeline"):].strip()

        fm = page.get("frontmatter") or {}
        fm["generated_by_llm"] = True
        self.upsert_page(slug, page["title"], page["page_type"], f"# {page['title']}\n\n{generated}", fm)

        # Auto-create cross-reference links by scanning generated content
        generated_lower = generated.lower()
        all_pages = self.list_pages()
        links_created = 0
        for p in all_pages:
            if p["slug"] == slug or p["slug"].startswith("_"):
                continue
            # Check if this page's title appears in the generated content
            if p["title"].lower() in generated_lower:
                self.add_link(slug, p["slug"], "mentions")
                links_created += 1
            # Check reverse: does the generated page's title appear in other pages?
            other = self.get_page(p["slug"])
            if other and page["title"].lower() in other.get("content", "").lower():
                self.add_link(p["slug"], slug, "mentions")

        # Update index
        index_content = generate_index(self.list_pages())
        self.upsert_page("_index", "Wiki Index", "index", index_content)

        self.append_log("generate", f"LLM-generated structured page for '{page['title']}', {links_created} links created")

        return {"slug": slug, "title": page["title"], "generated": True, "links_created": links_created}

    # ── Disambiguation (R3.10) ──────────────────────────────────

    def create_disambiguation_page(self, term: str, entries: list[dict]) -> dict:
        """Create a disambiguation page. entries: [{slug, meaning}]."""
        slug = f"disambig-{_slugify(term)}"
        lines = [
            f"# {term.title()} (disambiguation)",
            "",
            f"**{term.title()}** may refer to:",
            "",
        ]
        for entry in entries:
            lines.append(f"- [[{entry['slug']}|{entry.get('meaning', entry['slug'])}]]")
        lines.append("")

        content = "\n".join(lines)
        self.upsert_page(slug, f"{term.title()} (disambiguation)", "disambiguation", content)

        # Link disambiguation to each entry
        for entry in entries:
            self.add_link(slug, entry["slug"], "disambiguates")

        self.append_log("disambig", f"Created disambiguation for '{term}': {len(entries)} entries")
        return {"slug": slug, "entries": len(entries)}

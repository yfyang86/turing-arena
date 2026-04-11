"""Laureate agent manager. Loads SKILL.md files, builds system prompts, BM25 search."""

from __future__ import annotations
import math
import os
import re
from pathlib import Path
from dataclasses import dataclass, field

# Master registry of all 81 laureates with metadata
LAUREATES_REGISTRY: list[dict] = [
    {"slug": "alan-j-perlis", "name": "Alan J. Perlis", "year": 1966, "achievement": "ALGOL, compilers", "era": "foundation"},
    {"slug": "maurice-v-wilkes", "name": "Maurice V. Wilkes", "year": 1967, "achievement": "EDSAC, microprogramming", "era": "foundation"},
    {"slug": "richard-w-hamming", "name": "Richard W. Hamming", "year": 1968, "achievement": "Error-correcting codes", "era": "foundation"},
    {"slug": "marvin-minsky", "name": "Marvin Minsky", "year": 1969, "achievement": "AI, neural networks", "era": "foundation"},
    {"slug": "james-h-wilkinson", "name": "James H. Wilkinson", "year": 1970, "achievement": "Numerical analysis", "era": "foundation"},
    {"slug": "john-mccarthy", "name": "John McCarthy", "year": 1971, "achievement": "LISP, AI", "era": "foundation"},
    {"slug": "edsger-w-dijkstra", "name": "Edsger W. Dijkstra", "year": 1972, "achievement": "Structured programming", "era": "foundation"},
    {"slug": "charles-w-bachman", "name": "Charles W. Bachman", "year": 1973, "achievement": "Database systems", "era": "foundation"},
    {"slug": "donald-e-knuth", "name": "Donald E. Knuth", "year": 1974, "achievement": "TAOCP, algorithms", "era": "foundation"},
    {"slug": "allen-newell", "name": "Allen Newell", "year": 1975, "achievement": "AI, cognitive science", "era": "foundation"},
    {"slug": "herbert-a-simon", "name": "Herbert A. Simon", "year": 1975, "achievement": "AI, bounded rationality", "era": "foundation"},
    {"slug": "michael-o-rabin", "name": "Michael O. Rabin", "year": 1976, "achievement": "Automata, algorithms", "era": "foundation"},
    {"slug": "dana-s-scott", "name": "Dana S. Scott", "year": 1976, "achievement": "Semantics, domain theory", "era": "foundation"},
    {"slug": "john-backus", "name": "John Backus", "year": 1977, "achievement": "Fortran, FP", "era": "foundation"},
    {"slug": "robert-w-floyd", "name": "Robert W. Floyd", "year": 1978, "achievement": "Program verification", "era": "foundation"},
    {"slug": "kenneth-e-iverson", "name": "Kenneth E. Iverson", "year": 1979, "achievement": "APL language", "era": "foundation"},
    {"slug": "car-hoare", "name": "C.A.R. Hoare", "year": 1980, "achievement": "Hoare logic, Quicksort", "era": "foundation"},
    {"slug": "edgar-f-codd", "name": "Edgar F. Codd", "year": 1981, "achievement": "Relational databases", "era": "systems"},
    {"slug": "stephen-a-cook", "name": "Stephen A. Cook", "year": 1982, "achievement": "NP-completeness", "era": "systems"},
    {"slug": "ken-thompson", "name": "Ken Thompson", "year": 1983, "achievement": "Unix OS", "era": "systems"},
    {"slug": "dennis-ritchie", "name": "Dennis Ritchie", "year": 1983, "achievement": "C/Unix", "era": "systems"},
    {"slug": "niklaus-wirth", "name": "Niklaus Wirth", "year": 1984, "achievement": "Pascal, Modula", "era": "systems"},
    {"slug": "richard-m-karp", "name": "Richard M. Karp", "year": 1985, "achievement": "Algorithms, complexity", "era": "systems"},
    {"slug": "john-hopcroft", "name": "John Hopcroft", "year": 1986, "achievement": "Algorithms", "era": "systems"},
    {"slug": "robert-tarjan", "name": "Robert Tarjan", "year": 1986, "achievement": "Graph algorithms", "era": "systems"},
    {"slug": "john-cocke", "name": "John Cocke", "year": 1987, "achievement": "RISC architecture", "era": "systems"},
    {"slug": "ivan-sutherland", "name": "Ivan Sutherland", "year": 1988, "achievement": "Computer graphics", "era": "systems"},
    {"slug": "william-kahan", "name": "William Kahan", "year": 1989, "achievement": "Floating-point", "era": "systems"},
    {"slug": "fernando-j-corbato", "name": "Fernando J. Corbato", "year": 1990, "achievement": "Time-sharing", "era": "systems"},
    {"slug": "robin-milner", "name": "Robin Milner", "year": 1991, "achievement": "ML language", "era": "systems"},
    {"slug": "butler-w-lampson", "name": "Butler W. Lampson", "year": 1992, "achievement": "Distributed systems", "era": "systems"},
    {"slug": "juris-hartmanis", "name": "Juris Hartmanis", "year": 1993, "achievement": "Complexity theory", "era": "systems"},
    {"slug": "richard-e-stearns", "name": "Richard E. Stearns", "year": 1993, "achievement": "Complexity theory", "era": "systems"},
    {"slug": "edward-a-feigenbaum", "name": "Edward A. Feigenbaum", "year": 1994, "achievement": "Expert systems", "era": "systems"},
    {"slug": "raj-reddy", "name": "Raj Reddy", "year": 1994, "achievement": "Speech recognition", "era": "systems"},
    {"slug": "manuel-blum", "name": "Manuel Blum", "year": 1995, "achievement": "Complexity, cryptography", "era": "systems"},
    {"slug": "amir-pnueli", "name": "Amir Pnueli", "year": 1996, "achievement": "Temporal logic", "era": "internet"},
    {"slug": "douglas-engelbart", "name": "Douglas Engelbart", "year": 1997, "achievement": "Mouse, hypertext", "era": "internet"},
    {"slug": "jim-gray", "name": "Jim Gray", "year": 1998, "achievement": "Database transactions", "era": "internet"},
    {"slug": "frederick-p-brooks", "name": "Frederick P. Brooks", "year": 1999, "achievement": "Software engineering", "era": "internet"},
    {"slug": "andrew-chi-chih-yao", "name": "Andrew Yao", "year": 2000, "achievement": "Computational theory", "era": "internet"},
    {"slug": "ole-johan-dahl", "name": "Ole-Johan Dahl", "year": 2001, "achievement": "OOP, Simula", "era": "internet"},
    {"slug": "kristen-nygaard", "name": "Kristen Nygaard", "year": 2001, "achievement": "OOP, Simula", "era": "internet"},
    {"slug": "ronald-l-rivest", "name": "Ronald Rivest", "year": 2002, "achievement": "RSA cryptography", "era": "internet"},
    {"slug": "adi-shamir", "name": "Adi Shamir", "year": 2002, "achievement": "RSA cryptography", "era": "internet"},
    {"slug": "leonard-m-adleman", "name": "Leonard Adleman", "year": 2002, "achievement": "RSA, DNA computing", "era": "internet"},
    {"slug": "alan-kay", "name": "Alan Kay", "year": 2003, "achievement": "Smalltalk, OOP", "era": "internet"},
    {"slug": "vinton-g-cerf", "name": "Vint Cerf", "year": 2004, "achievement": "TCP/IP", "era": "internet"},
    {"slug": "robert-e-kahn", "name": "Bob Kahn", "year": 2004, "achievement": "TCP/IP", "era": "internet"},
    {"slug": "peter-naur", "name": "Peter Naur", "year": 2005, "achievement": "ALGOL 60, BNF", "era": "internet"},
    {"slug": "frances-e-allen", "name": "Frances Allen", "year": 2006, "achievement": "Compilers", "era": "internet"},
    {"slug": "edmund-m-clarke", "name": "Edmund Clarke", "year": 2007, "achievement": "Model checking", "era": "internet"},
    {"slug": "e-allen-emerson", "name": "Allen Emerson", "year": 2007, "achievement": "Model checking", "era": "internet"},
    {"slug": "joseph-sifakis", "name": "Joseph Sifakis", "year": 2007, "achievement": "Model checking", "era": "internet"},
    {"slug": "barbara-liskov", "name": "Barbara Liskov", "year": 2008, "achievement": "Distributed systems", "era": "internet"},
    {"slug": "charles-p-thacker", "name": "Charles Thacker", "year": 2009, "achievement": "Alto PC", "era": "internet"},
    {"slug": "leslie-valiant", "name": "Leslie Valiant", "year": 2010, "achievement": "PAC learning", "era": "internet"},
    {"slug": "judea-pearl", "name": "Judea Pearl", "year": 2011, "achievement": "Causal inference", "era": "modern"},
    {"slug": "shafi-goldwasser", "name": "Shafi Goldwasser", "year": 2012, "achievement": "Cryptography", "era": "modern"},
    {"slug": "silvio-micali", "name": "Silvio Micali", "year": 2012, "achievement": "Cryptography", "era": "modern"},
    {"slug": "leslie-lamport", "name": "Leslie Lamport", "year": 2013, "achievement": "Distributed systems", "era": "modern"},
    {"slug": "michael-stonebraker", "name": "Michael Stonebraker", "year": 2014, "achievement": "PostgreSQL", "era": "modern"},
    {"slug": "whitfield-diffie", "name": "Whitfield Diffie", "year": 2015, "achievement": "Public-key crypto", "era": "modern"},
    {"slug": "martin-hellman", "name": "Martin Hellman", "year": 2015, "achievement": "Public-key crypto", "era": "modern"},
    {"slug": "tim-berners-lee", "name": "Tim Berners-Lee", "year": 2016, "achievement": "World Wide Web", "era": "modern"},
    {"slug": "john-l-hennessy", "name": "John Hennessy", "year": 2017, "achievement": "RISC architecture", "era": "modern"},
    {"slug": "david-a-patterson", "name": "David Patterson", "year": 2017, "achievement": "RISC architecture", "era": "modern"},
    {"slug": "yoshua-bengio", "name": "Yoshua Bengio", "year": 2018, "achievement": "Deep learning", "era": "modern"},
    {"slug": "geoffrey-hinton", "name": "Geoffrey Hinton", "year": 2018, "achievement": "Deep learning", "era": "modern"},
    {"slug": "yann-le-cun", "name": "Yann LeCun", "year": 2018, "achievement": "Deep learning", "era": "modern"},
    {"slug": "edwin-catmull", "name": "Ed Catmull", "year": 2019, "achievement": "Pixar, graphics", "era": "modern"},
    {"slug": "patrick-hanrahan", "name": "Pat Hanrahan", "year": 2019, "achievement": "Rendering", "era": "modern"},
    {"slug": "alfred-v-aho", "name": "Alfred Aho", "year": 2020, "achievement": "Compilers", "era": "modern"},
    {"slug": "jeffrey-d-ullman", "name": "Jeffrey Ullman", "year": 2020, "achievement": "Automata theory", "era": "modern"},
    {"slug": "jack-dongarra", "name": "Jack Dongarra", "year": 2021, "achievement": "HPC, LAPACK", "era": "modern"},
    {"slug": "bob-metcalfe", "name": "Bob Metcalfe", "year": 2022, "achievement": "Ethernet", "era": "modern"},
    {"slug": "avi-wigderson", "name": "Avi Wigderson", "year": 2023, "achievement": "Complexity theory", "era": "modern"},
    {"slug": "andrew-g-barto", "name": "Andrew Barto", "year": 2024, "achievement": "Reinforcement learning", "era": "modern"},
    {"slug": "richard-s-sutton", "name": "Richard Sutton", "year": 2024, "achievement": "Reinforcement learning", "era": "modern"},
    {"slug": "charles-h-bennett", "name": "Charles Bennett", "year": 2025, "achievement": "Quantum crypto", "era": "modern"},
    {"slug": "gilles-brassard", "name": "Gilles Brassard", "year": 2025, "achievement": "Quantum crypto", "era": "modern"},
]


@dataclass
class LaureateAgent:
    slug: str
    name: str
    year: int
    achievement: str
    era: str
    skill_md: str = ""
    initials: str = ""

    def __post_init__(self):
        parts = self.name.replace(".", "").split()
        if len(parts) >= 2:
            self.initials = parts[0][0] + parts[-1][0]
        else:
            self.initials = self.name[:2].upper()

    def system_prompt(self) -> str:
        base = (
            f"You are {self.name}, {self.year} Turing Award laureate, known for: {self.achievement}.\n"
            f"Respond in character — use the cognitive framework, mental models, and expression style described below.\n"
            f"Stay in persona. Be opinionated from your unique perspective. Keep responses focused and engaging.\n"
            f"Do NOT break character or mention being an AI.\n\n"
        )
        if self.skill_md:
            return base + "=== COGNITIVE FRAMEWORK ===\n" + self.skill_md
        return base + "Use your known thinking style, publications, and worldview to respond authentically."

    def to_dict(self) -> dict:
        return {
            "slug": self.slug,
            "name": self.name,
            "year": self.year,
            "achievement": self.achievement,
            "era": self.era,
            "initials": self.initials,
        }


class AgentManager:
    def __init__(self, turingskill_path: str):
        self.base_path = Path(turingskill_path)
        self.agents: dict[str, LaureateAgent] = {}
        self._load_registry()
        self._bm25 = BM25Index()
        self._build_search_index()

    def _load_registry(self):
        for entry in LAUREATES_REGISTRY:
            agent = LaureateAgent(
                slug=entry["slug"],
                name=entry["name"],
                year=entry["year"],
                achievement=entry["achievement"],
                era=entry["era"],
            )
            # Try to load SKILL.md
            skill_path = self.base_path / entry["slug"] / "SKILL.md"
            if skill_path.exists():
                try:
                    agent.skill_md = skill_path.read_text(encoding="utf-8")[:12000]
                except Exception:
                    pass
            self.agents[entry["slug"]] = agent

    def _build_search_index(self):
        """Build BM25 index over laureate metadata."""
        for slug, agent in self.agents.items():
            # Combine searchable fields with field boosting via repetition
            text = " ".join([
                agent.name, agent.name,  # name boosted 2x
                agent.achievement, agent.achievement,  # achievement boosted 2x
                agent.era,
                str(agent.year),
                agent.slug.replace("-", " "),
            ])
            self._bm25.add_document(slug, text)
        self._bm25.finalize()

    def get(self, slug: str) -> LaureateAgent | None:
        return self.agents.get(slug)

    def list_all(self) -> list[dict]:
        return [a.to_dict() for a in self.agents.values()]

    def list_by_era(self) -> dict[str, list[dict]]:
        eras: dict[str, list[dict]] = {}
        for a in self.agents.values():
            eras.setdefault(a.era, []).append(a.to_dict())
        return eras

    def search(self, query: str, limit: int = 20) -> list[dict]:
        """BM25 search over laureates. Returns ranked list of dicts."""
        if not query.strip():
            return self.list_all()[:limit]
        # L31: Chinese name resolution
        query = self._resolve_chinese(query)
        results = self._bm25.search(query, top_k=limit)
        out = []
        for slug, score in results:
            agent = self.agents.get(slug)
            if agent:
                d = agent.to_dict()
                d["score"] = round(score, 3)
                out.append(d)
        return out

    # L31: Chinese name mapping
    CHINESE_NAMES: dict[str, str] = {
        "姚期智": "andrew-chi-chih-yao", "高德纳": "donald-e-knuth",
        "丘齐彻": "andrew-chi-chih-yao", "图灵": "turing",
        "迪杰斯特拉": "edsger-w-dijkstra", "辛顿": "geoffrey-hinton",
        "本吉奥": "yoshua-bengio", "勒昆": "yann-le-cun",
        "珀尔": "judea-pearl", "兰波特": "leslie-lamport",
        "里维斯特": "ronald-l-rivest", "伯纳斯-李": "tim-berners-lee",
    }

    def _resolve_chinese(self, query: str) -> str:
        for cn, slug in self.CHINESE_NAMES.items():
            if cn in query:
                agent = self.agents.get(slug)
                if agent:
                    query = query.replace(cn, agent.name)
        return query

    def resolve_name(self, name: str) -> LaureateAgent | None:
        """Resolve any name form (full, last, Chinese, slug) to an agent."""
        # Direct slug
        if name in self.agents:
            return self.agents[name]
        # Chinese
        for cn, slug in self.CHINESE_NAMES.items():
            if cn in name:
                return self.agents.get(slug)
        # Name match (case-insensitive)
        nl = name.lower()
        for a in self.agents.values():
            if nl == a.name.lower() or nl == a.name.split()[-1].lower():
                return a
        return None

    # L12: Topic-Match auto-recommend
    TOPIC_DOMAINS: dict[str, list[str]] = {
        "algorithm": ["donald-e-knuth", "robert-tarjan", "richard-m-karp", "edsger-w-dijkstra"],
        "ai": ["marvin-minsky", "john-mccarthy", "geoffrey-hinton", "yoshua-bengio", "yann-le-cun"],
        "machine learning": ["geoffrey-hinton", "yoshua-bengio", "yann-le-cun", "leslie-valiant"],
        "reinforcement learning": ["andrew-g-barto", "richard-s-sutton"],
        "database": ["edgar-f-codd", "michael-stonebraker", "jim-gray", "charles-w-bachman"],
        "cryptography": ["ronald-l-rivest", "adi-shamir", "whitfield-diffie", "martin-hellman", "shafi-goldwasser"],
        "quantum": ["charles-h-bennett", "gilles-brassard"],
        "systems": ["ken-thompson", "dennis-ritchie", "butler-w-lampson", "leslie-lamport"],
        "programming language": ["john-backus", "niklaus-wirth", "robin-milner", "alan-kay", "alan-j-perlis"],
        "graphics": ["ivan-sutherland", "edwin-catmull", "patrick-hanrahan"],
        "network": ["vinton-g-cerf", "robert-e-kahn", "bob-metcalfe", "tim-berners-lee"],
        "complexity": ["stephen-a-cook", "richard-m-karp", "juris-hartmanis", "avi-wigderson"],
        "software engineering": ["frederick-p-brooks", "car-hoare", "edsger-w-dijkstra", "barbara-liskov"],
        "causal": ["judea-pearl"],
        "formal verification": ["amir-pnueli", "edmund-m-clarke", "e-allen-emerson", "leslie-lamport"],
        "compiler": ["alfred-v-aho", "jeffrey-d-ullman", "frances-e-allen", "john-cocke"],
        "architecture": ["john-l-hennessy", "david-a-patterson", "john-cocke"],
        "numerical": ["james-h-wilkinson", "william-kahan", "jack-dongarra"],
    }

    def recommend_for_topic(self, topic: str, limit: int = 3) -> list[dict]:
        """L12: Recommend laureates by topic keywords."""
        tl = topic.lower()
        scores: dict[str, float] = {}
        for domain, slugs in self.TOPIC_DOMAINS.items():
            if any(kw in tl for kw in domain.split()):
                for s in slugs:
                    scores[s] = scores.get(s, 0) + 1.0
        # Also use BM25 search
        for slug, score in self._bm25.search(topic, top_k=10):
            scores[slug] = scores.get(slug, 0) + score * 0.3
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:limit]
        return [self.agents[s].to_dict() for s, _ in ranked if s in self.agents]

    # L13: Era Clash / Thinking Duel presets
    THINKING_STYLES: dict[str, list[str]] = {
        "contrarian": ["geoffrey-hinton", "charles-w-bachman", "edsger-w-dijkstra"],
        "systems": ["leslie-lamport", "barbara-liskov", "frederick-p-brooks"],
        "theoretical": ["stephen-a-cook", "richard-m-karp", "avi-wigderson", "andrew-chi-chih-yao"],
        "pragmatic": ["ken-thompson", "dennis-ritchie", "bob-metcalfe"],
        "interdisciplinary": ["herbert-a-simon", "allen-newell", "richard-s-sutton"],
        "long_term": ["donald-e-knuth", "kenneth-e-iverson", "ivan-sutherland"],
    }

    def era_clash(self) -> list[dict]:
        """L13: Pick one from Foundation + one from Modern era."""
        import random
        f = [a for a in self.agents.values() if a.era == "foundation"]
        m = [a for a in self.agents.values() if a.era == "modern"]
        return [random.choice(f).to_dict(), random.choice(m).to_dict()] if f and m else []

    def thinking_duel(self) -> list[dict]:
        """L13: Pick two laureates with opposing thinking styles."""
        import random
        styles = list(self.THINKING_STYLES.keys())
        random.shuffle(styles)
        picks = []
        for style in styles[:2]:
            slugs = [s for s in self.THINKING_STYLES[style] if s in self.agents]
            if slugs:
                picks.append(self.agents[random.choice(slugs)].to_dict())
        return picks

    def get_thinking_style(self, slug: str) -> str | None:
        """Return thinking style for a laureate."""
        for style, slugs in self.THINKING_STYLES.items():
            if slug in slugs:
                return style
        return None


# ========================================================
#  BM25 Index (pure Python, no dependencies)
# ========================================================

_TOKENIZE_RE = re.compile(r"[a-zA-Z0-9\u4e00-\u9fff]+")


def _tokenize(text: str) -> list[str]:
    return [t.lower() for t in _TOKENIZE_RE.findall(text)]


class BM25Index:
    """Okapi BM25 ranking over a small corpus (< 10K docs)."""

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.doc_ids: list[str] = []
        self.doc_tokens: list[list[str]] = []
        self.doc_len: list[int] = []
        self.avgdl: float = 0.0
        self.N: int = 0
        # term -> set of doc indices
        self.df: dict[str, int] = {}
        # doc_index -> {term: count}
        self.tf: list[dict[str, int]] = []
        self._finalized = False

    def add_document(self, doc_id: str, text: str):
        tokens = _tokenize(text)
        idx = len(self.doc_ids)
        self.doc_ids.append(doc_id)
        self.doc_tokens.append(tokens)
        self.doc_len.append(len(tokens))
        # Term frequency
        tf: dict[str, int] = {}
        for t in tokens:
            tf[t] = tf.get(t, 0) + 1
        self.tf.append(tf)
        # Document frequency
        for t in set(tokens):
            self.df[t] = self.df.get(t, 0) + 1

    def finalize(self):
        self.N = len(self.doc_ids)
        self.avgdl = sum(self.doc_len) / max(self.N, 1)
        self._finalized = True

    def _idf(self, term: str) -> float:
        df = self.df.get(term, 0)
        if df == 0:
            return 0.0
        return math.log((self.N - df + 0.5) / (df + 0.5) + 1.0)

    def search(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        if not self._finalized:
            self.finalize()
        q_tokens = _tokenize(query)
        if not q_tokens:
            return []

        scores: list[float] = [0.0] * self.N
        for term in q_tokens:
            idf = self._idf(term)
            if idf <= 0:
                continue
            for i in range(self.N):
                tf_val = self.tf[i].get(term, 0)
                if tf_val == 0:
                    continue
                dl = self.doc_len[i]
                numerator = tf_val * (self.k1 + 1)
                denominator = tf_val + self.k1 * (1 - self.b + self.b * dl / self.avgdl)
                scores[i] += idf * numerator / denominator

        # Rank
        ranked = [(self.doc_ids[i], scores[i]) for i in range(self.N) if scores[i] > 0]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked[:top_k]

"""Topic extraction from messages. Keyword-based (no LLM dependency)."""

from __future__ import annotations
import re
from collections import Counter

# Domain keywords that map to trackable topics
TOPIC_KEYWORDS: dict[str, list[str]] = {
    "algorithm": ["algorithm", "sorting", "search", "graph", "tree", "complexity", "optimization", "heuristic"],
    "artificial intelligence": ["ai", "artificial intelligence", "machine learning", "neural network", "deep learning"],
    "database": ["database", "sql", "query", "relational", "transaction", "schema"],
    "cryptography": ["cryptography", "encryption", "rsa", "security", "zero-knowledge", "hash"],
    "programming language": ["programming language", "compiler", "type system", "syntax", "parser", "interpreter"],
    "distributed systems": ["distributed", "consensus", "replication", "fault tolerance", "paxos", "raft"],
    "operating system": ["operating system", "kernel", "process", "thread", "unix", "memory management"],
    "networking": ["network", "tcp", "ip", "protocol", "internet", "ethernet", "packet"],
    "computer graphics": ["graphics", "rendering", "3d", "pixel", "shader", "ray tracing"],
    "formal methods": ["formal verification", "model checking", "temporal logic", "proof", "correctness"],
    "software engineering": ["software engineering", "testing", "agile", "design pattern", "refactoring"],
    "quantum computing": ["quantum", "qubit", "superposition", "entanglement"],
    "reinforcement learning": ["reinforcement learning", "reward", "policy", "agent", "environment", "mdp"],
    "causal inference": ["causal", "causality", "bayesian", "counterfactual", "do-calculus"],
    "computer architecture": ["risc", "cisc", "cpu", "pipeline", "cache", "instruction set"],
    "complexity theory": ["np-complete", "p vs np", "polynomial", "decidability", "turing machine"],
    "natural language": ["nlp", "natural language", "parsing", "semantics", "language model"],
    "ethics": ["ethics", "bias", "fairness", "privacy", "alignment", "safety"],
}

_WORD_RE = re.compile(r"[a-zA-Z]{2,}")


def extract_topics(text: str) -> list[str]:
    """Extract topic names from message text. Returns list of topic strings."""
    text_lower = text.lower()
    found = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                found.append(topic)
                break
    return found


def extract_and_store(session_manager, message_id: str, session_id: str, content: str):
    """Extract topics from a message and store them."""
    topics = extract_topics(content)
    for topic_name in topics:
        topic_id = session_manager.add_topic(topic_name)
        session_manager.add_topic_mention(topic_id, message_id, session_id)

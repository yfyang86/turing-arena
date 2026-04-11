"""WebSocket chat handler. Challenge/synthesis/panel/debate modes, topics, heartbeat."""

from __future__ import annotations
import asyncio
import json
import logging
import time
import tornado.websocket
import tornado.ioloop
from core.topic_tracker import extract_and_store

log = logging.getLogger(__name__)


class ChatWSHandler(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True

    def open(self):
        self.session_id: str | None = None
        self._closed = False
        self._last_pong = time.time()
        # L27: start heartbeat
        self._heartbeat = tornado.ioloop.PeriodicCallback(self._ping, 30000)
        self._heartbeat.start()

    def _ping(self):
        if self._closed:
            self._heartbeat.stop()
            return
        try:
            self.ping(b"hb")
        except Exception:
            self._closed = True
            self._heartbeat.stop()

    def on_pong(self, data):
        self._last_pong = time.time()

    def _safe_write(self, data: dict):
        if self._closed: return
        try:
            self.write_message(json.dumps(data))
        except tornado.websocket.WebSocketClosedError:
            self._closed = True

    def on_message(self, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            self._safe_write({"type": "error", "content": "Invalid JSON"})
            return
        # L27: client heartbeat
        if msg.get("action") == "ping":
            self._safe_write({"type": "pong"})
            return
        tornado.ioloop.IOLoop.current().spawn_callback(self._handle, msg)

    async def _handle(self, msg: dict):
        action = msg.get("action")
        if "session_id" in msg and msg["session_id"]:
            self.session_id = msg["session_id"]
        actions = {
            "join": self._send_history,
            "send": self._on_user_message,
            "debate": self._on_debate,
            "panel": self._on_panel,
            "challenge": self._on_challenge,
        }
        handler = actions.get(action)
        if handler:
            if action == "join":
                self.session_id = msg.get("session_id")
                await handler()
            else:
                await handler(msg)

    async def _send_history(self):
        if not self.session_id: return
        sm = self.application.settings["session_manager"]
        self._safe_write({
            "type": "history",
            "messages": sm.get_messages(self.session_id),
            "laureates": sm.get_session_laureates(self.session_id),
        })

    def _get_typing_delay(self) -> float:
        """L6: configurable typing delay."""
        ms = self.application.settings["config"].get("arena", {}).get("typing_delay_ms", 800)
        return ms / 1000.0

    async def _stream_agent(self, slug, conv_messages, sm, am, llm):
        """Common streaming logic. Returns full response text."""
        agent = am.get(slug)
        if not agent: return ""
        self._safe_write({"type": "typing", "laureate_slug": slug})
        await asyncio.sleep(self._get_typing_delay())  # L6

        provider = llm.provider_for_laureate(slug)  # L30
        full = ""
        try:
            async for chunk in llm.chat_stream(conv_messages, system=agent.system_prompt(), provider_name=provider):
                if self._closed: break
                full += chunk
                self._safe_write({"type": "stream", "laureate_slug": slug, "chunk": chunk})
        except Exception as e:
            log.warning("LLM error %s: %s", slug, e)
            full = f"[Error: {e}]"
            self._safe_write({"type": "stream", "laureate_slug": slug, "chunk": full})
        if full:
            saved = sm.add_message(self.session_id, "laureate", full, laureate_slug=slug)
            sm.increment_interaction(slug)
            # L9: extract topics
            extract_and_store(sm, saved["id"], self.session_id, full)
            self._safe_write({"type": "stream_end", "laureate_slug": slug, "message": saved})
        return full

    async def _on_user_message(self, msg: dict):
        if not self.session_id: return
        sm = self.application.settings["session_manager"]
        am = self.application.settings["agent_manager"]
        llm = self.application.settings["llm_router"]

        content = msg.get("content", "").strip()
        file_context = msg.get("file_context")
        if file_context:
            content = f"{content}\n\n[Attached: {file_context.get('filename','')}]\n```\n{file_context.get('text','')}\n```"
        if not content: return

        user_msg = sm.add_message(self.session_id, "user", content)
        self._safe_write({"type": "message", "message": user_msg})
        extract_and_store(sm, user_msg["id"], self.session_id, content)  # L9

        target_slug = msg.get("target")
        slugs = sm.get_session_laureates(self.session_id)
        if not slugs:
            self._safe_write({"type": "system", "content": "No laureates. Add some from the left panel!"}); return
        targets = [target_slug] if target_slug and target_slug in slugs else slugs

        history = sm.get_messages(self.session_id, limit=40)
        conv = []
        for m in history:
            if m["role"] == "user": conv.append({"role": "user", "content": m["content"]})
            elif m["role"] == "laureate":
                a = am.get(m.get("laureate_slug") or "")
                conv.append({"role": "assistant", "content": f"[{a.name if a else m.get('laureate_slug','')}]: {m['content']}"})

        for slug in targets:
            if self._closed: break
            agent_conv = list(conv)
            if len(targets) > 1:
                agent = am.get(slug)
                agent_conv.append({"role": "user", "content": f"[System: You are {agent.name if agent else slug}. Respond in your unique voice.]"})
            await self._stream_agent(slug, agent_conv, sm, am, llm)

    async def _run_rounds(self, slugs, history, sm, am, llm, max_rounds, prompt_fn):
        """Generic multi-round conversation loop."""
        for rnd in range(max_rounds):
            if self._closed: break
            for slug in slugs:
                if self._closed: break
                agent = am.get(slug)
                if not agent: continue
                prompt = prompt_fn(agent, rnd, max_rounds, slugs)
                conv = history + [{"role": "user", "content": prompt}]
                full = await self._stream_agent(slug, conv, sm, am, llm)
                if full:
                    history.append({"role": "assistant", "content": f"[{agent.name}]: {full}"})

    async def _on_debate(self, msg: dict):
        if not self.session_id: return
        sm, am, llm = (self.application.settings[k] for k in ("session_manager", "agent_manager", "llm_router"))
        config = self.application.settings["config"]
        topic = msg.get("content", "").strip()
        if not topic: return
        slugs = sm.get_session_laureates(self.session_id)
        if len(slugs) < 2:
            self._safe_write({"type": "system", "content": "Debate needs ≥2 laureates."}); return

        user_msg = sm.add_message(self.session_id, "user", f"[DEBATE] {topic}")
        self._safe_write({"type": "message", "message": user_msg})
        max_r = config.get("arena", {}).get("debate_max_rounds", 3)
        history = [{"role": "user", "content": f"Debate topic: {topic}"}]

        def prompt(agent, rnd, mx, _slugs):
            return f"[Round {rnd+1}/{mx}. Debate. Build on or challenge previous speakers. 2-3 paragraphs max.]"

        await self._run_rounds(slugs, history, sm, am, llm, max_r, prompt)
        # L2: Synthesis summary
        await self._generate_synthesis(history, slugs, sm, am, llm)
        self._safe_write({"type": "debate_end"})

    async def _on_panel(self, msg: dict):
        if not self.session_id: return
        sm, am, llm = (self.application.settings[k] for k in ("session_manager", "agent_manager", "llm_router"))
        config = self.application.settings["config"]
        topic = msg.get("content", "").strip()
        if not topic: return
        slugs = sm.get_session_laureates(self.session_id)
        if len(slugs) < 2:
            self._safe_write({"type": "system", "content": "Panel needs ≥2 laureates."}); return

        user_msg = sm.add_message(self.session_id, "user", f"[PANEL] {topic}")
        self._safe_write({"type": "message", "message": user_msg})
        self._safe_write({"type": "panel_start"})
        max_r = config.get("arena", {}).get("debate_max_rounds", 3)
        history = [{"role": "user", "content": f"Panel topic: {topic}"}]

        def prompt(agent, rnd, mx, all_slugs):
            others = [am.get(s).name for s in all_slugs if s != agent.slug and am.get(s)]
            return f"[Panel round {rnd+1}/{mx}. Discuss with {', '.join(others)}. Address them directly. 1-2 paragraphs.]"

        await self._run_rounds(slugs, history, sm, am, llm, max_r, prompt)
        self._safe_write({"type": "panel_end"})

    # L1: Challenge protocol
    async def _on_challenge(self, msg: dict):
        """Claim → Critique → Rebuttal between exactly 2 laureates."""
        if not self.session_id: return
        sm, am, llm = (self.application.settings[k] for k in ("session_manager", "agent_manager", "llm_router"))
        topic = msg.get("content", "").strip()
        slugs = msg.get("challengers", sm.get_session_laureates(self.session_id)[:2])
        if len(slugs) < 2:
            self._safe_write({"type": "system", "content": "Challenge needs 2 laureates."}); return

        user_msg = sm.add_message(self.session_id, "user", f"[CHALLENGE] {topic}")
        self._safe_write({"type": "message", "message": user_msg})
        self._safe_write({"type": "challenge_start"})

        a_slug, b_slug = slugs[0], slugs[1]
        a, b = am.get(a_slug), am.get(b_slug)
        history = [{"role": "user", "content": f"Topic: {topic}"}]

        # Round 1: A makes a claim
        conv = history + [{"role": "user", "content": f"[Make a bold claim about: {topic}. State your position clearly. 2 paragraphs.]"}]
        claim = await self._stream_agent(a_slug, conv, sm, am, llm)
        if claim: history.append({"role": "assistant", "content": f"[{a.name}]: {claim}"})

        # Round 2: B critiques
        if not self._closed:
            conv = history + [{"role": "user", "content": f"[Critique {a.name}'s claim. Find weaknesses. Be sharp. 2 paragraphs.]"}]
            critique = await self._stream_agent(b_slug, conv, sm, am, llm)
            if critique: history.append({"role": "assistant", "content": f"[{b.name}]: {critique}"})

        # Round 3: A rebuts
        if not self._closed:
            conv = history + [{"role": "user", "content": f"[Rebut {b.name}'s critique. Defend or refine your position. 2 paragraphs.]"}]
            rebuttal = await self._stream_agent(a_slug, conv, sm, am, llm)
            if rebuttal: history.append({"role": "assistant", "content": f"[{a.name}]: {rebuttal}"})

        self._safe_write({"type": "challenge_end"})

    # L2: Synthesis summary
    async def _generate_synthesis(self, history, slugs, sm, am, llm):
        """Generate a synthesis summary after a debate."""
        if self._closed or len(history) < 3: return
        names = [am.get(s).name for s in slugs if am.get(s)]
        synthesis_prompt = [
            {"role": "user", "content":
             f"Summarize the debate between {', '.join(names)}. In 2-3 paragraphs:\n"
             f"1. Key points of agreement\n2. Key points of disagreement\n3. Strongest argument made\n\n"
             + "\n".join(f"{m['role']}: {m['content']}" for m in history[-10:])}
        ]
        self._safe_write({"type": "typing", "laureate_slug": "_synthesis"})
        full = ""
        try:
            async for chunk in llm.chat_stream(synthesis_prompt, system="You are a neutral academic moderator. Synthesize the debate concisely."):
                if self._closed: break
                full += chunk
                self._safe_write({"type": "stream", "laureate_slug": "_synthesis", "chunk": chunk})
        except Exception as e:
            full = f"[Synthesis error: {e}]"
        if full:
            saved = sm.add_message(self.session_id, "system", f"[SYNTHESIS] {full}")
            self._safe_write({"type": "stream_end", "laureate_slug": "_synthesis", "message": saved})

    def on_close(self):
        self._closed = True
        self._heartbeat.stop()

/* ========================================================
   TuringMind Arena — Client Application (RC2)
   @mention, file upload, panel mode, mobile responsive
   ======================================================== */

const App = {
  ws: null,
  currentSession: null,
  laureates: [],
  laureatesBySlug: {},
  sessionLaureates: [],
  chatMode: 'ask',
  markdownEnabled: false,
  selectedInfo: null,
  streamBuffers: {},
  _searchTimeout: null,
  // File upload
  pendingFile: null,   // {filename, text, size}
  // @mention
  mentionActive: false,
  mentionQuery: '',
  mentionStart: -1,   // caret position where @ was typed
  mentionIdx: 0,      // highlighted option index
  mentionMatches: [],
  // Panel
  panelRunning: false,
  // Mobile
  mobilePanel: null,   // 'left' | 'right' | null

  /* ---------- Init ---------- */
  async init() {
    this.loadTheme();
    this.loadMarkdownSetting();
    await this.fetchLaureates();
    await this.refreshSessions();
    this.bindEvents();
    this.bindKeyboardShortcuts();
    this.connectWS();
    this.initKG();
    this.refreshKG();
  },

  /* ---------- Theme ---------- */
  loadTheme() {
    const t = localStorage.getItem('tm-theme') || 'dark';
    document.documentElement.setAttribute('data-theme', t);
    this.updateThemeIcon(t);
  },
  toggleTheme() {
    const cur = document.documentElement.getAttribute('data-theme');
    const next = cur === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('tm-theme', next);
    this.updateThemeIcon(next);
  },
  updateThemeIcon(t) {
    const el = document.getElementById('theme-icon');
    if (el) el.textContent = t === 'dark' ? '☀' : '☾';
  },

  /* ---------- Markdown Toggle ---------- */
  toggleMarkdown() {
    this.markdownEnabled = !this.markdownEnabled;
    localStorage.setItem('tm-markdown', this.markdownEnabled ? '1' : '0');
    const btn = document.getElementById('md-toggle');
    if (btn) btn.style.opacity = this.markdownEnabled ? '1' : '0.5';
    this.toast(this.markdownEnabled ? 'Markdown rendering enabled' : 'Markdown rendering disabled', 'info');
  },
  loadMarkdownSetting() {
    this.markdownEnabled = localStorage.getItem('tm-markdown') === '1';
    const btn = document.getElementById('md-toggle');
    if (btn) btn.style.opacity = this.markdownEnabled ? '1' : '0.5';
  },

  /* ---------- WebSocket ---------- */
  connectWS() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    this.ws = new WebSocket(`${proto}://${location.host}/ws/chat`);
    this.ws.onmessage = (e) => {
      try { this.onWSMessage(JSON.parse(e.data)); }
      catch (err) { console.error('WS parse error:', err); }
    };
    this.ws.onclose = () => setTimeout(() => this.connectWS(), 2000);
    this.ws.onopen = () => {
      if (this.currentSession) {
        this.wsSend({ action: 'join', session_id: this.currentSession.id });
      }
    };
  },

  wsSend(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  },

  onWSMessage(msg) {
    switch (msg.type) {
      case 'history':
        this.renderHistory(msg.messages);
        this.sessionLaureates = msg.laureates || [];
        this.renderActiveLaureates();
        this.markInArena();
        break;
      case 'message':
        this.clearEmptyState();
        this.appendMessage(msg.message);
        break;
      case 'typing':
        this.clearEmptyState();
        this.showTyping(msg.laureate_slug);
        break;
      case 'stream':
        this.handleStream(msg.laureate_slug, msg.chunk);
        break;
      case 'stream_end':
        this.finalizeStream(msg.laureate_slug, msg.message);
        break;
      case 'system':
        this.clearEmptyState();
        this.appendSystemMessage(msg.content);
        break;
      case 'debate_end':
        this.appendSystemMessage('Debate concluded.');
        break;
      case 'panel_start':
        this.panelRunning = true;
        document.getElementById('panel-banner').style.display = 'flex';
        document.getElementById('chat-input').disabled = true;
        document.getElementById('chat-input').placeholder = 'Observing panel discussion...';
        break;
      case 'panel_end':
        this.panelRunning = false;
        document.getElementById('panel-banner').style.display = 'none';
        document.getElementById('chat-input').disabled = false;
        document.getElementById('chat-input').placeholder = 'Ask a question, start a debate, or @mention a laureate...';
        this.appendSystemMessage('Panel discussion concluded.');
        break;
      case 'challenge_start':
        document.getElementById('challenge-banner').style.display = 'flex';
        break;
      case 'challenge_end':
        document.getElementById('challenge-banner').style.display = 'none';
        this.appendSystemMessage('Challenge concluded.');
        break;
      case 'error':
        this.appendSystemMessage(msg.content || 'Unknown error');
        break;
    }
  },

  /* ---------- Data ---------- */
  async fetchLaureates() {
    const res = await fetch('/api/laureates');
    this.laureates = await res.json();
    this.laureatesBySlug = {};
    this.laureates.forEach(l => this.laureatesBySlug[l.slug] = l);
    this.renderLaureateList(this.laureates);
  },

  async refreshSessions() {
    const res = await fetch('/api/sessions');
    const sessions = await res.json();
    this.renderSessionList(sessions);
    if (sessions.length > 0 && !this.currentSession) {
      await this.switchSession(sessions[0].id, sessions[0].name);
    }
  },

  /* ---------- Search ---------- */
  async searchLaureates(query) {
    if (!query.trim()) { this.renderLaureateList(this.laureates); return; }
    try {
      const res = await fetch(`/api/laureates/search?q=${encodeURIComponent(query)}&limit=30`);
      this.renderLaureateList(await res.json(), true);
    } catch {
      const q = query.toLowerCase();
      this.renderLaureateList(this.laureates.filter(l =>
        l.name.toLowerCase().includes(q) || l.achievement.toLowerCase().includes(q) ||
        l.era.includes(q) || String(l.year).includes(q)
      ), true);
    }
  },

  /* ---------- Sessions ---------- */
  async createSession() {
    const name = 'Arena ' + new Date().toLocaleDateString('en', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    const res = await fetch('/api/sessions', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    const session = await res.json();
    await this.switchSession(session.id, session.name);
    await this.refreshSessions();
  },

  async switchSession(id, name) {
    this.currentSession = { id, name };
    document.getElementById('arena-title').textContent = name;
    document.getElementById('chat-messages').innerHTML = '';
    this.streamBuffers = {};
    document.querySelectorAll('.session-item').forEach(el => {
      el.classList.toggle('active', el.dataset.id === id);
    });
    try {
      const res = await fetch(`/api/sessions/${id}/laureates`);
      this.sessionLaureates = (await res.json()).map(l => l.slug);
    } catch { this.sessionLaureates = []; }
    this.renderActiveLaureates();
    this.markInArena();
    this.wsSend({ action: 'join', session_id: id });
  },

  async deleteSession(id) {
    await fetch(`/api/sessions/${id}`, { method: 'DELETE' });
    if (this.currentSession && this.currentSession.id === id) {
      this.currentSession = null;
      document.getElementById('chat-messages').innerHTML = '';
      document.getElementById('arena-title').textContent = 'Select or create a session';
    }
    await this.refreshSessions();
  },

  renderSessionList(sessions) {
    const c = document.getElementById('session-list');
    c.innerHTML = '';
    sessions.forEach(s => {
      const div = document.createElement('div');
      div.className = 'session-item' + (this.currentSession && this.currentSession.id === s.id ? ' active' : '');
      div.dataset.id = s.id;
      const time = new Date(s.updated_at).toLocaleDateString('en', { month: 'short', day: 'numeric' });
      div.innerHTML = `
        <span class="session-name">${this.esc(s.name)}</span>
        <span class="session-time">${time}</span>
        <span class="session-delete" onclick="event.stopPropagation(); App.deleteSession('${this.esc(s.id)}')" title="Delete">×</span>`;
      div.onclick = () => this.switchSession(s.id, s.name);
      div.ondblclick = (e) => { e.stopPropagation(); this.startRenameSession(s.id, s.name); };
      c.appendChild(div);
    });
  },

  /* ---------- Laureate List ---------- */
  renderLaureateList(list, isSearch = false) {
    const c = document.getElementById('laureate-list-container');
    c.innerHTML = '';
    if (isSearch) {
      list.forEach(l => c.appendChild(this._makeLaureateEl(l)));
      if (!list.length) c.innerHTML = '<div class="placeholder-text" style="padding:20px">No matches</div>';
      return;
    }
    const eras = { foundation: 'Foundation (1966–1980)', systems: 'Systems (1981–1995)', internet: 'Internet/AI (1996–2010)', modern: 'Modern (2011–2025)' };
    const grouped = {};
    list.forEach(l => { (grouped[l.era] ??= []).push(l); });
    for (const [era, label] of Object.entries(eras)) {
      if (!grouped[era]) continue;
      const t = document.createElement('div');
      t.className = `era-group-title ${era}`;
      t.textContent = label;
      c.appendChild(t);
      grouped[era].forEach(l => c.appendChild(this._makeLaureateEl(l)));
    }
  },

  _makeLaureateEl(l) {
    const div = document.createElement('div');
    div.className = 'laureate-item';
    div.dataset.slug = l.slug;
    div.innerHTML = `
      <div class="laureate-avatar"><img src="${this.avatarUrl(l.slug, 32)}" alt="${this.esc(l.initials)}"></div>
      <div class="laureate-info">
        <div class="laureate-name">${this.esc(l.name)}</div>
        <div class="laureate-meta">${l.year} · ${this.esc(l.achievement)}</div>
      </div>
      <span class="laureate-add-btn" onclick="event.stopPropagation(); App.addLaureate('${l.slug}')" title="Add to arena">+</span>`;
    div.onclick = () => this.showLaureateInfo(l.slug);
    return div;
  },

  markInArena() {
    document.querySelectorAll('.laureate-item').forEach(el => {
      el.classList.toggle('in-arena', this.sessionLaureates.includes(el.dataset.slug));
    });
  },

  /* ---------- Add/Remove Laureates ---------- */
  async addLaureate(slug) {
    if (!this.currentSession) await this.createSession();
    const res = await fetch(`/api/sessions/${this.currentSession.id}/laureates`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ slug }),
    });
    if (res.ok) {
      if (!this.sessionLaureates.includes(slug)) this.sessionLaureates.push(slug);
      this.renderActiveLaureates();
      this.markInArena();
      const l = this.laureatesBySlug[slug];
      if (l) this.appendSystemMessage(`${l.name} (${l.year}) joined the arena.`);
    } else {
      try { this.appendSystemMessage((await res.json()).error); }
      catch { this.appendSystemMessage('Could not add laureate.'); }
    }
  },

  async removeLaureate(slug) {
    if (!this.currentSession) return;
    await fetch(`/api/sessions/${this.currentSession.id}/laureates`, {
      method: 'DELETE', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ slug }),
    });
    this.sessionLaureates = this.sessionLaureates.filter(s => s !== slug);
    this.renderActiveLaureates();
    this.markInArena();
    const l = this.laureatesBySlug[slug];
    if (l) this.appendSystemMessage(`${l.name} left the arena.`);
    if (this.selectedInfo === slug) this.showLaureateInfo(slug);
  },

  renderActiveLaureates() {
    const bar = document.getElementById('active-laureates');
    bar.innerHTML = '';
    this.sessionLaureates.forEach(slug => {
      const img = document.createElement('img');
      img.className = 'active-avatar';
      img.src = this.avatarUrl(slug, 28);
      img.title = (this.laureatesBySlug[slug] || {}).name || slug;
      img.onclick = () => this.showLaureateInfo(slug);
      img.onmouseenter = (e) => this.showAvatarTooltip(e, slug);
      img.onmouseleave = () => this.hideAvatarTooltip();
      bar.appendChild(img);
    });
  },

  /* ---------- Right Panel ---------- */
  showLaureateInfo(slug) {
    this.selectedInfo = slug;
    this.rightTab = 'info';
    const l = this.laureatesBySlug[slug];
    if (!l) return;
    const panel = document.getElementById('right-content');
    const inArena = this.sessionLaureates.includes(slug);
    panel.innerHTML = `
      <div class="info-card">
        <img class="info-avatar" src="${this.avatarUrl(slug, 80)}" alt="${this.esc(l.initials)}">
        <div class="info-name">${this.esc(l.name)}</div>
        <div class="info-year">${l.year} Turing Award</div>
        <div class="info-achievement">${this.esc(l.achievement)}</div>
        <div style="margin-top:10px"><span class="era-badge ${l.era}">${l.era}</span></div>
      </div>
      <div class="info-section">
        <div class="info-section-title">Arena Actions</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          ${inArena
            ? `<button class="btn btn-sm btn-danger" onclick="App.removeLaureate('${slug}')">Remove</button>`
            : `<button class="btn btn-sm btn-primary" onclick="App.addLaureate('${slug}')">Add to Arena</button>`}
        </div>
      </div>
      <div class="info-section">
        <div class="info-section-title">Session</div>
        <div class="export-btns">
          <button class="btn btn-sm btn-ghost" onclick="App.exportSession('md')">Export MD</button>
          <button class="btn btn-sm btn-ghost" onclick="App.exportSession('json')">Export JSON</button>
        </div>
      </div>`;
  },

  /* ---------- Chat Messages ---------- */
  clearEmptyState() {
    const el = document.querySelector('#chat-messages .empty-state');
    if (el) el.remove();
  },

  renderHistory(messages) {
    const c = document.getElementById('chat-messages');
    c.innerHTML = '';
    if (!messages.length) {
      c.innerHTML = `<div class="empty-state"><div class="empty-icon" style="font-family:var(--font-display);font-size:28px;opacity:0.4">TM</div><div class="empty-title">Welcome to TuringMind Arena</div><div class="empty-sub">Add laureates from the left panel and start chatting. Try the Spin button!</div></div>`;
      return;
    }
    messages.forEach(m => this.appendMessage(m, false));
    this.scrollToBottom();
  },

  appendMessage(m, scroll = true) {
    this.clearEmptyState();
    const c = document.getElementById('chat-messages');
    const div = document.createElement('div');
    if (m.role === 'user') {
      div.className = 'message user';
      div.innerHTML = `
        <div class="msg-body">
          <div class="msg-content">${this.formatText(m.content)}</div>
          <div class="msg-time">${this.fmtTime(m.created_at)}</div>
        </div>
        <div class="msg-avatar" style="width:36px;height:36px;border-radius:50%;background:var(--accent-primary);display:flex;align-items:center;justify-content:center;color:var(--text-inverse);font-weight:600;font-size:14px;flex-shrink:0">U</div>`;
    } else if (m.role === 'laureate') {
      const l = this.laureatesBySlug[m.laureate_slug] || {};
      div.className = 'message laureate';
      div.innerHTML = `
        <img class="msg-avatar" src="${this.avatarUrl(m.laureate_slug, 36)}" alt="" onclick="App.showLaureateInfo('${m.laureate_slug}')">
        <div class="msg-body">
          <div class="msg-header">${this.esc(l.name || m.laureate_slug)}</div>
          <div class="msg-content">${this.formatText(m.content)}</div>
          <div class="msg-time">${this.fmtTime(m.created_at)}</div>
        </div>`;
    } else {
      div.className = 'message system';
      div.innerHTML = `<div class="msg-body">${this.formatText(m.content)}</div>`;
    }
    c.appendChild(div);
    if (scroll) this.scrollToBottom();
  },

  appendSystemMessage(text) {
    this.clearEmptyState();
    const c = document.getElementById('chat-messages');
    const div = document.createElement('div');
    div.className = 'message system';
    div.innerHTML = `<div class="msg-body">${this.esc(text)}</div>`;
    c.appendChild(div);
    this.scrollToBottom();
  },

  showTyping(slug) {
    this.clearTyping();
    const c = document.getElementById('chat-messages');
    const l = this.laureatesBySlug[slug] || {};
    const div = document.createElement('div');
    div.className = 'typing-indicator';
    div.id = 'typing-' + slug;
    div.innerHTML = `<img src="${this.avatarUrl(slug, 24)}" style="width:24px;height:24px;border-radius:50%"><span>${this.esc(l.name || slug)} is thinking</span><span class="typing-dots"><span></span><span></span><span></span></span>`;
    c.appendChild(div);
    this.scrollToBottom();
  },

  clearTyping() {
    document.querySelectorAll('.typing-indicator').forEach(el => el.remove());
  },

  handleStream(slug, chunk) {
    this.clearTyping();
    const isSynthesis = slug === '_synthesis';
    if (!this.streamBuffers[slug]) {
      this.streamBuffers[slug] = '';
      const c = document.getElementById('chat-messages');
      const l = this.laureatesBySlug[slug] || {};
      const div = document.createElement('div');
      div.className = isSynthesis ? 'message system' : 'message laureate';
      div.id = 'stream-' + slug;
      if (isSynthesis) {
        div.innerHTML = `<div class="msg-body"><div class="msg-header" style="color:var(--success)">Synthesis</div><div class="msg-content" id="stream-content-${slug}"></div></div>`;
      } else {
        div.innerHTML = `<img class="msg-avatar" src="${this.avatarUrl(slug, 36)}" alt="" onclick="App.showLaureateInfo('${slug}')"><div class="msg-body"><div class="msg-header">${this.esc(l.name || slug)}</div><div class="msg-content" id="stream-content-${slug}"></div></div>`;
      }
      c.appendChild(div);
    }
    this.streamBuffers[slug] += chunk;
    const el = document.getElementById('stream-content-' + slug);
    if (el) el.innerHTML = this.formatText(this.streamBuffers[slug]);
    this.scrollToBottom();
  },

  finalizeStream(slug, message) {
    const el = document.getElementById('stream-' + slug);
    if (el) el.remove();
    delete this.streamBuffers[slug];
    this.appendMessage(message);
  },

  /* ==========================================================
     @Mention Autocomplete
     ========================================================== */
  _showMentionDropdown() {
    const q = this.mentionQuery.toLowerCase();
    // Filter to laureates in current session
    this.mentionMatches = this.sessionLaureates
      .map(slug => this.laureatesBySlug[slug])
      .filter(l => l && (l.name.toLowerCase().includes(q) || l.slug.includes(q)))
      .slice(0, 8);

    const dd = document.getElementById('mention-dropdown');
    if (!this.mentionMatches.length) {
      dd.classList.remove('visible');
      return;
    }
    this.mentionIdx = 0;
    dd.innerHTML = '';
    this.mentionMatches.forEach((l, i) => {
      const opt = document.createElement('div');
      opt.className = 'mention-option' + (i === 0 ? ' highlighted' : '');
      opt.innerHTML = `<img src="${this.avatarUrl(l.slug, 28)}" alt=""><div><div class="mention-option-name">${this.esc(l.name)}</div><div class="mention-option-meta">${l.year} · ${this.esc(l.achievement)}</div></div>`;
      opt.onmouseenter = () => {
        dd.querySelectorAll('.mention-option').forEach(o => o.classList.remove('highlighted'));
        opt.classList.add('highlighted');
        this.mentionIdx = i;
      };
      opt.onclick = (e) => { e.preventDefault(); this._completeMention(l); };
      dd.appendChild(opt);
    });
    dd.classList.add('visible');
  },

  _hideMentionDropdown() {
    this.mentionActive = false;
    this.mentionQuery = '';
    this.mentionStart = -1;
    document.getElementById('mention-dropdown').classList.remove('visible');
  },

  _completeMention(laureate) {
    const input = document.getElementById('chat-input');
    const val = input.value;
    // Replace from @ to caret with @Name
    const before = val.substring(0, this.mentionStart);
    const after = val.substring(input.selectionStart);
    const mention = `@${laureate.name} `;
    input.value = before + mention + after;
    input.selectionStart = input.selectionEnd = before.length + mention.length;
    input.focus();
    this._hideMentionDropdown();
  },

  _handleMentionKey(e) {
    if (!this.mentionActive) return false;
    const dd = document.getElementById('mention-dropdown');
    if (!dd.classList.contains('visible')) return false;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      this.mentionIdx = Math.min(this.mentionIdx + 1, this.mentionMatches.length - 1);
      this._highlightMention();
      return true;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      this.mentionIdx = Math.max(this.mentionIdx - 1, 0);
      this._highlightMention();
      return true;
    }
    if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      if (this.mentionMatches[this.mentionIdx]) {
        this._completeMention(this.mentionMatches[this.mentionIdx]);
      }
      return true;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      this._hideMentionDropdown();
      return true;
    }
    return false;
  },

  _highlightMention() {
    const dd = document.getElementById('mention-dropdown');
    dd.querySelectorAll('.mention-option').forEach((o, i) => {
      o.classList.toggle('highlighted', i === this.mentionIdx);
    });
  },

  _checkMentionTrigger() {
    const input = document.getElementById('chat-input');
    const val = input.value;
    const pos = input.selectionStart;

    // Find the last @ before caret that isn't preceded by a word char
    let atPos = -1;
    for (let i = pos - 1; i >= 0; i--) {
      if (val[i] === '@' && (i === 0 || /\s/.test(val[i - 1]))) {
        atPos = i;
        break;
      }
      if (val[i] === ' ' || val[i] === '\n') break;
    }

    if (atPos >= 0) {
      this.mentionActive = true;
      this.mentionStart = atPos;
      this.mentionQuery = val.substring(atPos + 1, pos);
      this._showMentionDropdown();
    } else if (this.mentionActive) {
      this._hideMentionDropdown();
    }
  },

  /* ==========================================================
     File Upload
     ========================================================== */
  async uploadFile(file) {
    const form = new FormData();
    form.append('file', file);
    try {
      const res = await fetch('/api/upload', { method: 'POST', body: form });
      if (!res.ok) {
        const err = await res.json();
        this.appendSystemMessage(`Upload failed: ${err.error}`);
        return;
      }
      const data = await res.json();
      this.pendingFile = data;
      const indicator = document.getElementById('file-indicator');
      indicator.innerHTML = `Attached: ${this.esc(data.filename)} (${(data.size / 1024).toFixed(1)}KB) <span class="file-remove" onclick="App.clearFile()">×</span>`;
      indicator.style.display = 'flex';
    } catch (e) {
      this.appendSystemMessage(`Upload error: ${e.message}`);
    }
  },

  clearFile() {
    this.pendingFile = null;
    const indicator = document.getElementById('file-indicator');
    indicator.style.display = 'none';
    indicator.innerHTML = '';
    document.getElementById('file-input').value = '';
  },

  /* ==========================================================
     Send
     ========================================================== */
  send() {
    const input = document.getElementById('chat-input');
    const text = input.value.trim();
    if (!text && !this.pendingFile) return;
    input.value = '';
    input.style.height = 'auto';
    this._hideMentionDropdown();

    if (!this.currentSession) {
      this.createSession().then(() => this._doSend(text));
    } else {
      this._doSend(text);
    }
  },

  _doSend(text) {
    // Parse @mention target from text
    let target = null;
    const mentionMatch = text.match(/@([A-Z][a-zA-Z.\s]+?)(?=\s|$)/);
    if (mentionMatch) {
      const mentionName = mentionMatch[1].trim();
      for (const slug of this.sessionLaureates) {
        const l = this.laureatesBySlug[slug];
        if (l && l.name.toLowerCase().startsWith(mentionName.toLowerCase())) {
          target = slug; break;
        }
      }
    }

    const actionMap = { ask: 'send', debate: 'debate', panel: 'panel', challenge: 'challenge' };
    const payload = {
      action: actionMap[this.chatMode] || 'send',
      content: text || (this.pendingFile ? `Review this file: ${this.pendingFile.filename}` : ''),
      session_id: this.currentSession.id,
    };
    if (target) payload.target = target;
    if (this.pendingFile) {
      payload.file_context = { filename: this.pendingFile.filename, text: this.pendingFile.text };
    }
    this.wsSend(payload);
    this.clearFile();
  },

  /* ---------- Panel Mode ---------- */
  stopPanel() {
    // We can't actually stop the server-side loop, but we close and reconnect
    // which triggers _closed flag on server
    if (this.ws) this.ws.close();
    this.panelRunning = false;
    document.getElementById('panel-banner').style.display = 'none';
    document.getElementById('chat-input').disabled = false;
    document.getElementById('chat-input').placeholder = 'Ask a question, start a debate, or @mention a laureate...';
    this.appendSystemMessage('Panel discussion stopped.');
  },

  /* ---------- Spin the Wheel ---------- */
  spinWheel() {
    if (!this.currentSession) { this.createSession().then(() => this.spinWheel()); return; }
    const count = 2 + Math.floor(Math.random() * 2);
    const pool = this.laureates.filter(l => !this.sessionLaureates.includes(l.slug));
    if (pool.length < count) { this.appendSystemMessage('Not enough laureates to spin!'); return; }
    for (let i = pool.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [pool[i], pool[j]] = [pool[j], pool[i]];
    }
    const picked = pool.slice(0, count);

    const overlay = document.createElement('div');
    overlay.className = 'wheel-overlay'; overlay.id = 'wheel-overlay';
    overlay.innerHTML = `<div class="wheel-container"><h3>Spin the Wheel</h3><div class="spinning-text" style="font-size:24px;font-family:var(--font-display)">...</div><div class="wheel-result" id="wheel-result"></div><div style="margin-top:16px"><button class="btn btn-primary" id="wheel-confirm" style="display:none" onclick="App.confirmWheel()">Add to Arena</button> <button class="btn btn-ghost" onclick="App.closeWheel()">Cancel</button></div></div>`;
    document.body.appendChild(overlay);
    overlay.onclick = (e) => { if (e.target === overlay) this.closeWheel(); };
    this._wheelPicked = picked;
    picked.forEach((l, i) => {
      setTimeout(() => {
        const r = document.getElementById('wheel-result'); if (!r) return;
        const item = document.createElement('div');
        item.className = 'wheel-result-item'; item.style.animationDelay = `${i*0.1}s`;
        item.innerHTML = `<img src="${this.avatarUrl(l.slug, 36)}" alt=""><div><div class="wheel-result-name">${this.esc(l.name)}</div><div class="wheel-result-meta">${l.year} · ${this.esc(l.achievement)}</div></div>`;
        r.appendChild(item);
        if (i === picked.length - 1) {
          const s = document.querySelector('.spinning-text'); if (s) s.style.display = 'none';
          const b = document.getElementById('wheel-confirm'); if (b) b.style.display = '';
        }
      }, 800 + i * 600);
    });
  },
  confirmWheel() { if (this._wheelPicked) this._wheelPicked.forEach(l => this.addLaureate(l.slug)); this.closeWheel(); },
  closeWheel() { const o = document.getElementById('wheel-overlay'); if (o) o.remove(); this._wheelPicked = null; },

  /* ==========================================================
     Mobile Responsive
     ========================================================== */
  toggleMobilePanel(side) {
    const left = document.getElementById('left-panel');
    const right = document.getElementById('right-panel');

    // Close if same panel tapped again
    if (this.mobilePanel === side) {
      this.closeMobilePanel();
      return;
    }

    // Close any open panel first
    left.classList.remove('mobile-open');
    right.classList.remove('mobile-open');
    document.querySelectorAll('.mobile-backdrop').forEach(b => b.remove());

    // Open requested panel
    const panel = side === 'left' ? left : right;
    panel.classList.add('mobile-open');
    this.mobilePanel = side;

    // Backdrop
    const backdrop = document.createElement('div');
    backdrop.className = 'mobile-backdrop';
    backdrop.onclick = () => this.closeMobilePanel();
    document.body.appendChild(backdrop);

    // Toggle button state
    document.getElementById('mobile-left-btn').classList.toggle('active', side === 'left');
    document.getElementById('mobile-right-btn').classList.toggle('active', side === 'right');
  },

  closeMobilePanel() {
    document.getElementById('left-panel').classList.remove('mobile-open');
    document.getElementById('right-panel').classList.remove('mobile-open');
    document.querySelectorAll('.mobile-backdrop').forEach(b => b.remove());
    document.getElementById('mobile-left-btn').classList.remove('active');
    document.getElementById('mobile-right-btn').classList.remove('active');
    this.mobilePanel = null;
  },

  /* ---------- Events ---------- */
  bindEvents() {
    const input = document.getElementById('chat-input');

    // Chat input: Enter to send, @mention handling
    input.addEventListener('keydown', (e) => {
      // @mention takes priority
      if (this._handleMentionKey(e)) return;
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        this.send();
      }
    });

    input.addEventListener('input', () => {
      input.style.height = 'auto';
      input.style.height = Math.min(input.scrollHeight, 120) + 'px';
      this._checkMentionTrigger();
    });

    // Dismiss mention dropdown on blur (with delay for click)
    input.addEventListener('blur', () => {
      setTimeout(() => this._hideMentionDropdown(), 200);
    });

    // Search input
    const searchInput = document.getElementById('laureate-search');
    if (searchInput) {
      searchInput.addEventListener('input', () => {
        clearTimeout(this._searchTimeout);
        this._searchTimeout = setTimeout(() => this.searchLaureates(searchInput.value), 200);
      });
    }

    // File upload
    const fileInput = document.getElementById('file-input');
    if (fileInput) {
      fileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (file) this.uploadFile(file);
      });
    }

    // Mode toggle
    document.querySelectorAll('.mode-btn').forEach(btn => {
      btn.onclick = () => {
        document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.chatMode = btn.dataset.mode;
      };
    });
  },

  /* ---------- Helpers ---------- */
  // Cache-bust version: forces browser to re-fetch avatars after update
  _avatarVer: Date.now(),
  avatarUrl(slug, size) {
    return `/api/avatar/${slug}?size=${size}&v=${this._avatarVer}`;
  },

  scrollToBottom() {
    const el = document.getElementById('chat-messages');
    requestAnimationFrame(() => { el.scrollTop = el.scrollHeight; });
  },

  formatText(text) {
    if (!text) return '';

    // Full markdown rendering via marked.js (when enabled + available)
    if (this.markdownEnabled && typeof marked !== 'undefined') {
      try {
        let html = marked.parse(text, {
          breaks: true,
          gfm: true,
          sanitize: false,
        });
        // Highlight @mentions in rendered output
        html = html.replace(/@([A-Z][a-zA-Z.\s]+?)(?=\s|<|$)/g, '<span class="mention-tag">@$1</span>');
        return html;
      } catch (e) {
        // fallback to simple renderer on error
      }
    }

    // Simple renderer (default, no deps)
    let html = this.esc(text);
    html = html.replace(/```([\s\S]*?)```/g, '<pre><code>$1</code></pre>');
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');
    html = html.replace(/@([A-Z][a-zA-Z.\s]+?)(?=\s|<|$)/g, '<span class="mention-tag">@$1</span>');
    html = html.replace(/\n/g, '<br>');
    return html;
  },

  esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
  },

  // L7: Relative timestamps
  fmtTime(ts) {
    if (!ts) return '';
    try {
      const d = new Date(ts);
      if (isNaN(d.getTime())) return '';
      const now = Date.now(), diff = now - d.getTime();
      if (diff < 60000) return 'just now';
      if (diff < 3600000) return `${Math.floor(diff/60000)}m ago`;
      if (diff < 86400000) return `${Math.floor(diff/3600000)}h ago`;
      if (diff < 604800000) return `${Math.floor(diff/86400000)}d ago`;
      return d.toLocaleDateString('en', { month: 'short', day: 'numeric' });
    } catch { return ''; }
  },

  // ==========================================================
  //  L25: Toast Notifications
  // ==========================================================
  toast(msg, type = 'info') {
    const c = document.getElementById('toast-container');
    if (!c) return;
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    t.textContent = msg;
    c.appendChild(t);
    setTimeout(() => t.remove(), 4000);
  },

  // ==========================================================
  //  L22: Keyboard Shortcuts
  // ==========================================================
  _kbdHintVisible: false,
  bindKeyboardShortcuts() {
    document.addEventListener('keydown', (e) => {
      // Don't trigger in inputs
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
      if (e.key === 'n' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); this.createSession(); }
      if (e.key === '/' && !e.ctrlKey) { e.preventDefault(); document.getElementById('chat-input').focus(); }
      if (e.key === 'f' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); this.showMessageSearch(); }
      if (e.key === '?' && e.shiftKey) { this.toggleKbdHints(); }
      if (e.key === 'Escape') {
        this.closeMobilePanel();
        this.closeWheel();
        this._hideKbdHints();
      }
    });
  },
  toggleKbdHints() {
    if (this._kbdHintVisible) { this._hideKbdHints(); return; }
    const hint = document.createElement('div');
    hint.className = 'kbd-hint'; hint.id = 'kbd-hints';
    hint.innerHTML = `
      <div><kbd>/</kbd> Focus chat</div>
      <div><kbd>Ctrl+N</kbd> New session</div>
      <div><kbd>Ctrl+F</kbd> Search messages</div>
      <div><kbd>?</kbd> Toggle shortcuts</div>
      <div><kbd>Esc</kbd> Close panels</div>`;
    document.body.appendChild(hint);
    this._kbdHintVisible = true;
  },
  _hideKbdHints() {
    const h = document.getElementById('kbd-hints');
    if (h) h.remove();
    this._kbdHintVisible = false;
  },

  // ==========================================================
  //  L23: Session Rename Inline
  // ==========================================================
  startRenameSession(id, currentName) {
    const nameEl = document.querySelector(`.session-item[data-id="${id}"] .session-name`);
    if (!nameEl) return;
    const input = document.createElement('input');
    input.className = 'session-name-edit';
    input.value = currentName;
    input.onblur = () => this.finishRename(id, input.value);
    input.onkeydown = (e) => {
      if (e.key === 'Enter') input.blur();
      if (e.key === 'Escape') { input.value = currentName; input.blur(); }
    };
    nameEl.replaceWith(input);
    input.focus(); input.select();
  },
  async finishRename(id, newName) {
    if (newName.trim()) {
      await fetch(`/api/sessions/${id}`, {
        method: 'PATCH', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({name: newName.trim()})
      });
      if (this.currentSession && this.currentSession.id === id) {
        this.currentSession.name = newName.trim();
        document.getElementById('arena-title').textContent = newName.trim();
      }
    }
    this.refreshSessions();
  },

  // ==========================================================
  //  L14/L15/L16: Collection Gallery + Thinking Badge
  // ==========================================================
  rightTab: 'info',
  showRightTab(tab) {
    this.rightTab = tab;
    document.querySelectorAll('.rtab').forEach(b => b.classList.toggle('active', b.id === 'rtab-' + tab));
    if (tab === 'info') {
      if (this.selectedInfo) this.showLaureateInfo(this.selectedInfo);
      else document.getElementById('right-content').innerHTML = '<div class="placeholder-text">Click a laureate.</div>';
    } else if (tab === 'collection') {
      this.showCollection();
    } else if (tab === 'topics') {
      this.showTopics();
    } else if (tab === 'wiki') {
      this.showWiki('');
    }
  },
  async showCollection() {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      const [colRes, styleRes] = await Promise.all([fetch('/api/collection'), fetch('/api/user-style')]);
      const collection = await colRes.json();
      const style = await styleRes.json();
      let html = '';
      if (style.dominant) {
        html += `<div class="info-section"><div class="info-section-title">Your Thinking Style</div><div style="font-size:14px;font-weight:600;color:var(--accent-primary);text-transform:capitalize">${style.dominant}</div></div>`;
      }
      html += `<div class="info-section"><div class="info-section-title">Collection (${collection.filter(c=>c.unlocked).length}/${collection.length} unlocked)</div></div>`;
      html += '<div class="collection-grid">';
      // Show all laureates, dim locked ones
      const allL = this.laureates;
      const colMap = {};
      collection.forEach(c => colMap[c.slug] = c);
      for (const l of allL) {
        const c = colMap[l.slug];
        const unlocked = c && c.unlocked;
        const count = c ? c.interactions : 0;
        html += `<div class="collection-card ${unlocked ? '' : 'locked'}" onclick="App.showLaureateInfo('${l.slug}');App.showRightTab('info')">
          <img class="card-avatar" src="${this.avatarUrl(l.slug, 48)}" alt="">
          <div class="card-name">${this.esc(l.name.split(' ').pop())}</div>
          <div class="card-count">${count}/5</div>
          ${unlocked ? '<span class="card-badge">✓</span>' : ''}
        </div>`;
      }
      html += '</div>';
      panel.innerHTML = html;
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },

  // ==========================================================
  //  L9/L10/L11: Topics + Hype Cycle
  // ==========================================================
  async showTopics() {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      const res = await fetch('/api/topics');
      const topics = await res.json();
      let html = '';

      // Add topic input
      html += '<div class="wiki-actions">';
      html += `<input class="wiki-search-input" id="topic-add-input" placeholder="New topic..." style="flex:1" onkeydown="if(event.key==='Enter')App.addTopic()">`;
      html += `<button class="btn btn-sm btn-ghost" onclick="App.addTopic()">Add</button>`;
      html += '</div>';

      if (!topics.length) {
        html += '<div class="placeholder-text">No topics tracked yet. Add one above or start chatting.</div>';
      } else {
        html += '<div class="info-section"><div class="info-section-title">Tracked Topics</div></div>';
        topics.slice(0, 20).forEach(t => {
          html += `<div class="topic-list-item" onclick="App.showTopicDetail('${this.esc(t.name)}')">
            <span class="tli-name">${this.esc(t.name)}</span>
            <span class="tli-count">${t.mentions}</span>
          </div>`;
        });
        if (topics.length > 20) {
          html += `<div style="padding:8px 14px;font-size:11px;color:var(--text-muted)">Showing top 20 of ${topics.length} topics</div>`;
        }
      }
      panel.innerHTML = html;
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },

  async addTopic() {
    const input = document.getElementById('topic-add-input');
    if (!input) return;
    const name = input.value.trim();
    if (!name) { this.toast('Enter a topic name', 'info'); return; }
    try {
      const res = await fetch('/api/topics/add', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name })
      });
      const data = await res.json();
      if (data.error) { this.toast(data.error, 'error'); return; }
      this.toast(`Topic added: ${data.name}`, 'success');
      input.value = '';
      this.showTopics();
      this.refreshKG();
    } catch (e) { this.toast('Failed: ' + e.message, 'error'); }
  },

  async showTopicDetail(name) {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      const res = await fetch(`/api/topics/timeline?name=${encodeURIComponent(name)}`);
      const points = await res.json();

      let html = '';
      html += `<div style="padding:6px 14px;font-size:11px"><a style="cursor:pointer;color:var(--text-link)" onclick="App.showTopics()">Back to Topics</a></div>`;
      html += `<div class="topic-detail-header"><h3>${this.esc(name)}</h3>`;
      html += `<div class="td-meta">${points.length} mention${points.length !== 1 ? 's' : ''} across sessions</div></div>`;

      // Hype cycle mini chart
      if (points.length > 0) {
        html += '<div class="hype-cycle-container">' + this._renderHypeCycleSVG(name, points) + '</div>';
      }

      // Timeline entries
      if (points.length > 0) {
        html += '<div class="info-section"><div class="info-section-title">Session Timeline</div></div>';
        points.forEach(p => {
          html += `<div class="topic-timeline-entry">
            <div class="tte-header">
              <span class="tte-date">${(p.time || '').substring(0, 10)}</span>
              ${p.session ? `<span class="tte-session" onclick="App.switchSession('${this.esc(p.session)}','')">${this.esc(p.session)}</span>` : ''}
              ${p.laureate ? `<span style="font-size:10px;color:var(--text-muted)">${this.esc(p.laureate)}</span>` : ''}
            </div>
          </div>`;
        });
      } else {
        html += '<div class="placeholder-text">No timeline data for this topic.</div>';
      }

      // Link to wiki concept page if it exists
      const wikiSlug = 'concept-' + name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
      html += `<div class="info-section"><div class="info-section-title">Related</div>`;
      html += `<div style="padding:4px 14px"><a style="cursor:pointer;color:var(--text-link)" onclick="App.showRightTab('wiki');setTimeout(()=>App.wikiViewPage('${wikiSlug}'),100)">View wiki page</a></div></div>`;

      panel.innerHTML = html;
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },

  _renderHypeCycleSVG(name, points) {
    const w = 260, h = 100, pad = 20;
    const n = points.length;
    let svg = `<svg viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg">`;
    if (n > 1) {
      const buckets = Math.min(10, n);
      const bucketSize = Math.ceil(n / buckets);
      const vals = [];
      for (let i = 0; i < buckets; i++) vals.push(points.slice(i * bucketSize, (i + 1) * bucketSize).length);
      const maxV = Math.max(...vals, 1);
      const bx = vals.map((_, i) => pad + (i / (buckets - 1)) * (w - 2 * pad));
      const by = vals.map(v => h - pad - (v / maxV) * (h - 2 * pad));
      let path = `M${bx[0]},${h - pad}`;
      bx.forEach((x, i) => path += ` L${x},${by[i]}`);
      path += ` L${bx[bx.length-1]},${h - pad} Z`;
      svg += `<path d="${path}" fill="var(--accent-muted)" stroke="var(--accent-primary)" stroke-width="1.5"/>`;
      bx.forEach((x, i) => svg += `<circle cx="${x}" cy="${by[i]}" r="2.5" fill="var(--accent-primary)"/>`);
    } else {
      svg += `<circle cx="${w/2}" cy="${h/2}" r="4" fill="var(--accent-primary)"/>`;
      svg += `<text x="${w/2}" y="${h/2+16}" text-anchor="middle" fill="var(--text-muted)" font-size="10" font-family="var(--font-body)">1 mention</text>`;
    }
    svg += `<line x1="${pad}" y1="${h-pad}" x2="${w-pad}" y2="${h-pad}" stroke="var(--border-primary)" stroke-width="1"/>`;
    svg += '</svg>';
    return svg;
  },

  // ==========================================================
  //  L19: Message Search
  // ==========================================================
  showMessageSearch() {
    const existing = document.getElementById('msg-search-overlay');
    if (existing) { existing.remove(); return; }
    const overlay = document.createElement('div');
    overlay.id = 'msg-search-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:1000;display:flex;align-items:flex-start;justify-content:center;padding-top:80px';
    overlay.innerHTML = `<div style="background:var(--bg-elevated);border-radius:var(--radius-lg);padding:20px;width:500px;max-width:90vw;max-height:70vh;overflow-y:auto;box-shadow:var(--shadow-lg)">
      <input id="msg-search-input" type="text" placeholder="Search messages..." style="width:100%;padding:10px;border:1px solid var(--border-input);border-radius:var(--radius-md);background:var(--bg-input);color:var(--text-primary);font-size:14px;outline:none;margin-bottom:12px" autofocus>
      <div id="msg-search-results" style="font-size:13px;color:var(--text-secondary)"></div>
    </div>`;
    document.body.appendChild(overlay);
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    const input = document.getElementById('msg-search-input');
    let debounce;
    input.addEventListener('input', () => {
      clearTimeout(debounce);
      debounce = setTimeout(async () => {
        const q = input.value.trim();
        if (!q) { document.getElementById('msg-search-results').innerHTML = ''; return; }
        const res = await fetch(`/api/messages/search?q=${encodeURIComponent(q)}`);
        const results = await res.json();
        const el = document.getElementById('msg-search-results');
        if (!results.length) { el.innerHTML = '<div style="color:var(--text-muted);padding:8px">No results.</div>'; return; }
        el.innerHTML = results.map(r => `<div style="padding:8px;border-bottom:1px solid var(--border-subtle);cursor:pointer" onclick="App.switchSession('${r.session_id}','');document.getElementById('msg-search-overlay').remove()">
          <span style="color:var(--text-muted);font-size:10px">${r.role}${r.laureate_slug ? ' · '+r.laureate_slug : ''}</span><br>
          ${this.esc(r.content)}</div>`).join('');
      }, 300);
    });
    input.addEventListener('keydown', (e) => { if (e.key === 'Escape') overlay.remove(); });
  },

  // ==========================================================
  //  L18: Export
  // ==========================================================
  async exportSession(format) {
    if (!this.currentSession) return;
    const url = `/api/sessions/${this.currentSession.id}/export?format=${format}`;
    if (format === 'md') {
      window.open(url, '_blank');
    } else {
      const res = await fetch(url);
      const data = await res.json();
      const blob = new Blob([JSON.stringify(data, null, 2)], {type: 'application/json'});
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = `session-${this.currentSession.id}.json`;
      a.click();
    }
    this.toast('Session exported', 'success');
  },

  // ==========================================================
  //  Knowledge Graph Panel (d3.js force-directed, Obsidian-style)
  // ==========================================================
  _kgCollapsed: false,
  _kgSim: null,
  _kgNodes: [],
  _kgEdges: [],
  _kgHovered: null,
  _kgAvatarCache: {},  // slug → Image object
  _kgFilter: { concept: true, entity: true, session_summary: true, disambiguation: true },
  _kgSearchQuery: '',
  _kgLayout: 'force',
  _kgFocusNode: null,
  _kgFocusHops: 1,
  _kgClusterPalette: [
    '#c4956a', '#7EACB5', '#8b9dab', '#a88b6a', '#6b8f7a',
    '#9b7ab5', '#b5886b', '#6b9fb5', '#b57a7a', '#7ab58f',
  ],
  _kgPan: { x: 0, y: 0 },
  _kgScale: 1,
  _kgDragging: false,
  _kgDragStart: { x: 0, y: 0 },
  _kgPanStart: { x: 0, y: 0 },

  initKG() {
    const handle = document.getElementById('kg-resize-handle');
    const panel = document.getElementById('kg-panel');
    if (!handle || !panel) return;

    // Panel resize (drag top border)
    let resizing = false, resizeStartY = 0, resizeStartH = 0;
    handle.addEventListener('mousedown', (e) => {
      resizing = true; resizeStartY = e.clientY; resizeStartH = panel.offsetHeight;
      e.preventDefault();
    });
    document.addEventListener('mousemove', (e) => {
      if (!resizing) return;
      const rightPanel = document.getElementById('right-panel');
      const maxH = rightPanel ? Math.floor(rightPanel.offsetHeight * 0.7) : 400;
      const newH = Math.max(80, Math.min(maxH, resizeStartH + (resizeStartY - e.clientY)));
      panel.style.height = newH + 'px';
      this._resizeKGCanvas();
    });
    document.addEventListener('mouseup', () => { resizing = false; });

    // Canvas interactions
    const canvas = document.getElementById('kg-canvas');
    if (!canvas) return;

    // Wheel → zoom (around cursor)
    canvas.addEventListener('wheel', (e) => {
      e.preventDefault();
      const rect = canvas.getBoundingClientRect();
      const mx = e.clientX - rect.left, my = e.clientY - rect.top;
      const zoomFactor = e.deltaY < 0 ? 1.12 : 1 / 1.12;
      const newScale = Math.max(0.2, Math.min(5, this._kgScale * zoomFactor));
      // Zoom toward cursor: adjust pan so point under cursor stays fixed
      const ratio = newScale / this._kgScale;
      this._kgPan.x = mx - ratio * (mx - this._kgPan.x);
      this._kgPan.y = my - ratio * (my - this._kgPan.y);
      this._kgScale = newScale;
      this._drawKG();
    }, { passive: false });

    // Mouse drag → pan
    canvas.addEventListener('mousedown', (e) => {
      // Only pan with left button, not on a node (node click handled separately)
      const node = this._kgHitTest(e);
      if (node) return;  // let click handler deal with it
      this._kgDragging = true;
      this._kgDragStart = { x: e.clientX, y: e.clientY };
      this._kgPanStart = { x: this._kgPan.x, y: this._kgPan.y };
      canvas.style.cursor = 'grabbing';
      e.preventDefault();
    });
    document.addEventListener('mousemove', (e) => {
      if (!this._kgDragging) return;
      this._kgPan.x = this._kgPanStart.x + (e.clientX - this._kgDragStart.x);
      this._kgPan.y = this._kgPanStart.y + (e.clientY - this._kgDragStart.y);
      this._drawKG();
    });
    document.addEventListener('mouseup', () => {
      if (this._kgDragging) {
        this._kgDragging = false;
        const canvas = document.getElementById('kg-canvas');
        if (canvas) canvas.style.cursor = 'default';
      }
    });

    // Hover + click
    canvas.addEventListener('mousemove', (e) => {
      if (this._kgDragging) return;
      this._kgMouseMove(e);
    });
    canvas.addEventListener('click', (e) => this._kgClick(e));
    canvas.addEventListener('mouseleave', () => this._kgClearTooltip());

    // Double-click → reset view (clear focus if active, otherwise fit all)
    canvas.addEventListener('dblclick', (e) => {
      e.preventDefault();
      if (this._kgFocusNode) {
        this.kgClearFocus();
      } else {
        this._kgFitAll();
      }
    });
  },

  toggleKG() {
    const panel = document.getElementById('kg-panel');
    const btn = document.getElementById('kg-toggle');
    this._kgCollapsed = !this._kgCollapsed;
    panel.classList.toggle('collapsed', this._kgCollapsed);
    btn.textContent = this._kgCollapsed ? '▴' : '▾';
    if (!this._kgCollapsed) this.refreshKG();
  },

  _resizeKGCanvas() {
    const canvas = document.getElementById('kg-canvas');
    const body = document.getElementById('kg-body');
    if (!canvas || !body) return;
    canvas.width = body.clientWidth * (window.devicePixelRatio || 1);
    canvas.height = body.clientHeight * (window.devicePixelRatio || 1);
    canvas.style.width = body.clientWidth + 'px';
    canvas.style.height = body.clientHeight + 'px';
    if (this._kgNodes.length) this._drawKG();
  },

  async refreshKG() {
    if (this._kgCollapsed) return;
    try {
      let url = '/api/wiki/graph';
      if (this._kgFocusNode) {
        url += `?focus=${encodeURIComponent(this._kgFocusNode)}&hops=${this._kgFocusHops}`;
      }
      const res = await fetch(url);
      const data = await res.json();
      if (!data.nodes.length) return;
      this._runKGSimulation(data);
    } catch (e) { /* silently fail */ }
  },

  kgSearch(query) {
    this._kgSearchQuery = query.toLowerCase().trim();
    this._drawKG();
  },

  kgToggleFilter(type, btn) {
    this._kgFilter[type] = !this._kgFilter[type];
    btn.classList.toggle('active', this._kgFilter[type]);
    this._drawKG();
  },

  kgSetLayout(mode) {
    this._kgLayout = mode;
    if (mode === 'force') {
      this._runKGSimulation({ nodes: this._kgNodes.map(n => ({...n})), edges: this._kgEdges.map(e => ({source: e.source.id || e.source, target: e.target.id || e.target, type: e.type})) });
    } else if (mode === 'radial') {
      this._kgLayoutRadial();
    } else if (mode === 'hierarchical') {
      this._kgLayoutHierarchical();
    }
  },

  kgFocusOn(slug) {
    this._kgFocusNode = slug;
    this._kgFocusHops = 1;
    this.refreshKG();
    this.toast(`Focused on ${slug} (1-hop)`, 'info');
  },

  kgClearFocus() {
    this._kgFocusNode = null;
    this.refreshKG();
  },

  kgExportPNG() {
    const canvas = document.getElementById('kg-canvas');
    if (!canvas) return;
    const link = document.createElement('a');
    link.download = 'turingmind-knowledge-graph.png';
    link.href = canvas.toDataURL('image/png');
    link.click();
    this.toast('Knowledge graph exported as PNG', 'success');
  },

  _getKGColors() {
    const cs = getComputedStyle(document.documentElement);
    const v = (name) => cs.getPropertyValue(name).trim();
    return {
      bg: v('--kg-bg'), edge: v('--kg-edge'), edgeHover: v('--kg-edge-hover'),
      label: v('--kg-label'), glow: v('--kg-glow'),
      concept: v('--kg-node-concept'), entity: v('--kg-node-entity'),
      session: v('--kg-node-session'), def: v('--kg-node-default'),
    };
  },

  _nodeColor(n) {
    const c = this._getKGColors();
    // Use cluster color when available and more than 1 cluster exists
    const hasClusters = this._kgNodes.some(x => x.cluster > 0);
    if (hasClusters && n.cluster !== undefined) {
      return this._kgClusterPalette[n.cluster % this._kgClusterPalette.length];
    }
    return { concept: c.concept, entity: c.entity, session_summary: c.session }[n.type] || c.def;
  },

  _runKGSimulation(data) {
    const nodes = data.nodes.map(n => ({...n, x: undefined, y: undefined}));
    const nodeMap = {};
    nodes.forEach(n => nodeMap[n.id] = n);
    const edges = data.edges
      .filter(e => nodeMap[e.source] && nodeMap[e.target])
      .map(e => ({source: nodeMap[e.source], target: nodeMap[e.target], type: e.type}));

    this._kgNodes = nodes;
    this._kgEdges = edges;

    // Preload avatar images for entity nodes
    for (const n of nodes) {
      if (n.type === 'entity' && !this._kgAvatarCache[n.id]) {
        const slug = n.id.replace(/^entity-/, '');
        const img = new Image();
        img.onload = () => {
          this._kgAvatarCache[n.id] = img;
          this._drawKG();  // redraw when image arrives
        };
        img.src = this.avatarUrl(slug, 64);
      }
    }

    this._resizeKGCanvas();

    const canvas = document.getElementById('kg-canvas');
    if (!canvas) return;
    const w = canvas.width, h = canvas.height;

    if (this._kgSim) this._kgSim.stop();

    if (typeof d3 === 'undefined') { this._drawKGFallback(); return; }

    this._kgSim = d3.forceSimulation(nodes)
      .force('charge', d3.forceManyBody().strength(-80))
      .force('link', d3.forceLink(edges).id(d => d.id).distance(50))
      .force('center', d3.forceCenter(w / 2, h / 2))
      .force('collision', d3.forceCollide().radius(d => Math.min(6 + d.degree * 1.5, 22)))
      .alphaDecay(0.03)
      .on('tick', () => this._drawKG());

    setTimeout(() => {
      if (this._kgSim) this._kgSim.stop();
      this._kgFitAll();
    }, 3000);
  },

  _drawKG() {
    const canvas = document.getElementById('kg-canvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const w = canvas.width, h = canvas.height;
    const dpr = window.devicePixelRatio || 1;
    const colors = this._getKGColors();
    const s = this._kgScale, px = this._kgPan.x, py = this._kgPan.y;
    const sq = this._kgSearchQuery;

    // Build visible set (filtering R3.4 + search R3.3)
    const visibleSet = new Set();
    const matchedSet = new Set();
    for (const n of this._kgNodes) {
      if (!this._kgFilter[n.type]) continue;
      const isMatch = sq && (n.label.toLowerCase().includes(sq) || n.id.toLowerCase().includes(sq));
      if (isMatch) matchedSet.add(n.id);
      if (!sq || isMatch) visibleSet.add(n.id);
    }
    // Search: also show neighbors of matched nodes
    if (sq) {
      for (const e of this._kgEdges) {
        const sid = e.source.id || e.source, tid = e.target.id || e.target;
        if (matchedSet.has(sid)) visibleSet.add(tid);
        if (matchedSet.has(tid)) visibleSet.add(sid);
      }
    }

    ctx.clearRect(0, 0, w, h);
    ctx.save();
    ctx.setTransform(s * dpr, 0, 0, s * dpr, px * dpr, py * dpr);

    // Edges
    for (const e of this._kgEdges) {
      const sid = e.source.id || e.source, tid = e.target.id || e.target;
      if (!visibleSet.has(sid) || !visibleSet.has(tid)) continue;
      const isHover = this._kgHovered && (sid === this._kgHovered.id || tid === this._kgHovered.id);
      ctx.strokeStyle = isHover ? colors.edgeHover : colors.edge;
      ctx.lineWidth = (isHover ? 1.5 : 0.8) / s;
      ctx.beginPath(); ctx.moveTo(e.source.x, e.source.y); ctx.lineTo(e.target.x, e.target.y); ctx.stroke();
    }

    // Nodes
    for (const n of this._kgNodes) {
      if (!visibleSet.has(n.id)) continue;
      const r = Math.min(3 + n.degree * 1.2, 16) / s * Math.min(s, 1.5);
      const color = this._nodeColor(n);
      const isHover = this._kgHovered && this._kgHovered.id === n.id;
      const isMatch = matchedSet.has(n.id);
      const avatarImg = this._kgAvatarCache[n.id];

      // Search highlight ring (R3.3)
      if (isMatch) {
        ctx.beginPath(); ctx.arc(n.x, n.y, r * 2, 0, Math.PI * 2);
        ctx.fillStyle = 'rgba(255,220,80,0.15)'; ctx.fill();
        ctx.strokeStyle = 'rgba(255,220,80,0.6)'; ctx.lineWidth = 1.5 / s; ctx.stroke();
      }
      // Glow
      if (isHover) {
        ctx.beginPath(); ctx.arc(n.x, n.y, r * 2.5, 0, Math.PI * 2);
        ctx.fillStyle = colors.glow; ctx.fill();
      }
      // Node: portrait or circle
      if (avatarImg && avatarImg.complete) {
        ctx.save(); ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2); ctx.clip();
        ctx.globalAlpha = isHover ? 1.0 : 0.85;
        ctx.drawImage(avatarImg, n.x - r, n.y - r, r * 2, r * 2);
        ctx.globalAlpha = 1.0; ctx.restore();
        ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
        ctx.strokeStyle = isHover ? color : colors.edge; ctx.lineWidth = (isHover ? 1.5 : 0.8) / s; ctx.stroke();
      } else {
        ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
        ctx.fillStyle = color; ctx.globalAlpha = isHover || isMatch ? 1.0 : 0.75;
        ctx.fill(); ctx.globalAlpha = 1.0;
      }
      // Label
      if (n.degree >= 2 || isHover || isMatch || s >= 1.5) {
        const fs = Math.max(7, (isHover ? 11 : 9) / s * Math.min(s, 1.2));
        ctx.font = `${fs}px ${getComputedStyle(document.documentElement).getPropertyValue('--font-body').trim()}`;
        ctx.fillStyle = isMatch ? 'rgba(255,220,80,0.9)' : colors.label;
        ctx.textAlign = 'center';
        ctx.fillText(n.label.length > 20 ? n.label.substring(0, 18) + '…' : n.label, n.x, n.y + r + fs + 2);
      }
    }

    ctx.restore();
    // Focus indicator
    if (this._kgFocusNode) {
      ctx.save(); ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.font = `10px sans-serif`; ctx.fillStyle = 'rgba(255,220,80,0.7)'; ctx.textAlign = 'left';
      ctx.fillText(`Focus: ${this._kgFocusNode} · dbl-click to clear`, 6, 14);
      ctx.restore();
    }
  },

  _kgFitAll() {
    /**Fit all nodes into view with padding.*/
    if (!this._kgNodes.length) return;
    const canvas = document.getElementById('kg-canvas');
    if (!canvas) return;
    const body = document.getElementById('kg-body');
    const cw = body ? body.clientWidth : canvas.width;
    const ch = body ? body.clientHeight : canvas.height;
    const pad = 30;

    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (const n of this._kgNodes) {
      if (n.x < minX) minX = n.x;
      if (n.x > maxX) maxX = n.x;
      if (n.y < minY) minY = n.y;
      if (n.y > maxY) maxY = n.y;
    }
    const gw = maxX - minX || 1, gh = maxY - minY || 1;
    const scale = Math.min((cw - pad * 2) / gw, (ch - pad * 2) / gh, 2.0);
    const cx = (minX + maxX) / 2, cy = (minY + maxY) / 2;
    this._kgScale = scale;
    this._kgPan.x = cw / 2 - cx * scale;
    this._kgPan.y = ch / 2 - cy * scale;
    this._drawKG();
  },

  _drawKGFallback() {
    const canvas = document.getElementById('kg-canvas');
    if (!canvas) return;
    const body = document.getElementById('kg-body');
    const cw = body ? body.clientWidth : 300;
    const ch = body ? body.clientHeight : 200;
    const nodes = this._kgNodes;
    const cx = cw / 2, cy = ch / 2;
    nodes.forEach((n, i) => {
      const angle = (i / nodes.length) * Math.PI * 2;
      const dist = Math.min(cw, ch) * 0.35;
      n.x = cx + Math.cos(angle) * dist;
      n.y = cy + Math.sin(angle) * dist;
    });
    this._kgFitAll();
  },

  _kgLayoutRadial() {
    /** Radial layout: highest-degree node at center, rings by BFS distance. */
    if (!this._kgNodes.length) return;
    if (this._kgSim) this._kgSim.stop();

    const body = document.getElementById('kg-body');
    const cw = body ? body.clientWidth : 300;
    const ch = body ? body.clientHeight : 200;
    const cx = cw / 2, cy = ch / 2;

    // Find root: highest degree
    const root = this._kgNodes.reduce((a, b) => a.degree >= b.degree ? a : b);

    // BFS for distance from root
    const adj = {};
    this._kgNodes.forEach(n => adj[n.id] = []);
    this._kgEdges.forEach(e => {
      const s = e.source.id || e.source, t = e.target.id || e.target;
      if (adj[s]) adj[s].push(t);
      if (adj[t]) adj[t].push(s);
    });
    const dist = {}; dist[root.id] = 0;
    const queue = [root.id];
    while (queue.length) {
      const cur = queue.shift();
      for (const nb of (adj[cur] || [])) {
        if (dist[nb] === undefined) { dist[nb] = dist[cur] + 1; queue.push(nb); }
      }
    }

    // Group by ring, place evenly
    const maxDist = Math.max(...Object.values(dist), 1);
    const ringSpacing = Math.min(cw, ch) * 0.35 / maxDist;
    const byRing = {};
    this._kgNodes.forEach(n => {
      const d = dist[n.id] !== undefined ? dist[n.id] : maxDist;
      (byRing[d] = byRing[d] || []).push(n);
    });

    for (const [d, nodes] of Object.entries(byRing)) {
      const r = (+d) * ringSpacing;
      nodes.forEach((n, i) => {
        const angle = (i / nodes.length) * Math.PI * 2 - Math.PI / 2;
        n.x = cx + Math.cos(angle) * r;
        n.y = cy + Math.sin(angle) * r;
      });
    }
    this._kgFitAll();
  },

  _kgLayoutHierarchical() {
    /** Hierarchical layout: sessions top, concepts middle, entities bottom. */
    if (!this._kgNodes.length) return;
    if (this._kgSim) this._kgSim.stop();

    const body = document.getElementById('kg-body');
    const cw = body ? body.clientWidth : 300;
    const ch = body ? body.clientHeight : 200;

    const tiers = { session_summary: [], concept: [], entity: [], disambiguation: [], other: [] };
    this._kgNodes.forEach(n => {
      const t = tiers[n.type] || tiers.other;
      t.push(n);
    });

    const order = ['session_summary', 'concept', 'entity', 'disambiguation', 'other'];
    const nonEmpty = order.filter(t => tiers[t].length > 0);
    const tierH = ch / (nonEmpty.length + 1);

    nonEmpty.forEach((tierName, ti) => {
      const nodes = tiers[tierName];
      const y = tierH * (ti + 1);
      const spacing = cw / (nodes.length + 1);
      nodes.forEach((n, i) => {
        n.x = spacing * (i + 1);
        n.y = y;
      });
    });
    this._kgFitAll();
  },

  _kgHitTest(e) {
    const canvas = document.getElementById('kg-canvas');
    if (!canvas) return null;
    const rect = canvas.getBoundingClientRect();
    // Screen coords → graph coords (inverse of pan+zoom)
    const sx = e.clientX - rect.left, sy = e.clientY - rect.top;
    const gx = (sx - this._kgPan.x) / this._kgScale;
    const gy = (sy - this._kgPan.y) / this._kgScale;
    const hitRadius = 8 / this._kgScale;  // constant hit area in screen space
    for (const n of this._kgNodes) {
      const r = Math.min(3 + n.degree * 1.2, 16) / this._kgScale * Math.min(this._kgScale, 1.5) + hitRadius;
      const dx = n.x - gx, dy = n.y - gy;
      if (dx * dx + dy * dy < r * r) return n;
    }
    return null;
  },

  _kgMouseMove(e) {
    const node = this._kgHitTest(e);
    const canvas = document.getElementById('kg-canvas');
    if (canvas) canvas.style.cursor = node ? 'pointer' : 'grab';
    if (node !== this._kgHovered) {
      this._kgHovered = node;
      this._drawKG();
      this._kgClearTooltip();
      if (node) {
        const tt = document.createElement('div');
        tt.className = 'kg-tooltip'; tt.id = 'kg-tooltip';
        tt.innerHTML = `<div>${this.esc(node.label)}</div><div class="kgt-type">${node.type} · v${node.version} · ${node.degree} links</div>`;
        tt.style.left = (e.clientX + 12) + 'px';
        tt.style.top = (e.clientY - 8) + 'px';
        document.body.appendChild(tt);
      }
    }
  },

  _kgClick(e) {
    const node = this._kgHitTest(e);
    if (!node) return;
    if (e.shiftKey) {
      // Shift+click: focus on this node's subgraph
      this.kgFocusOn(node.id);
    } else {
      this.showRightTab('wiki');
      setTimeout(() => this.wikiViewPage(node.id), 100);
    }
  },

  _kgClearTooltip() {
    const tt = document.getElementById('kg-tooltip');
    if (tt) tt.remove();
  },

  // ==========================================================
  //  L8: Avatar Tooltip
  // ==========================================================
  _tooltip: null,
  showAvatarTooltip(e, slug) {
    const l = this.laureatesBySlug[slug];
    if (!l) return;
    this.hideAvatarTooltip();
    const tt = document.createElement('div');
    tt.className = 'avatar-tooltip'; tt.id = 'avatar-tooltip';
    tt.innerHTML = `<div class="tt-name">${this.esc(l.name)}</div><div class="tt-meta">${l.year} · ${this.esc(l.achievement)}</div>`;
    tt.style.left = e.pageX + 10 + 'px';
    tt.style.top = e.pageY - 30 + 'px';
    document.body.appendChild(tt);
  },
  hideAvatarTooltip() {
    const t = document.getElementById('avatar-tooltip');
    if (t) t.remove();
  },

  // ==========================================================
  //  Wiki Feature
  // ==========================================================
  async showWiki(searchQuery) {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      const isSearch = searchQuery && searchQuery.trim();
      const [statsRes, pagesRes] = await Promise.all([
        fetch('/api/wiki/stats'),
        isSearch
          ? fetch(`/api/wiki/search?q=${encodeURIComponent(searchQuery)}`)
          : fetch('/api/wiki/pages'),
      ]);
      const stats = await statsRes.json();
      const pages = await pagesRes.json();

      let html = '';

      // Search bar
      html += `<div class="wiki-search-row">
        <input class="wiki-search-input" id="wiki-search-input"
               placeholder="Search wiki pages..."
               value="${this.esc(searchQuery || '')}"
               oninput="clearTimeout(App._wikiSearchTimeout); App._wikiSearchTimeout = setTimeout(() => App.showWiki(this.value), 250)">
      </div>`;

      // Actions
      html += '<div class="wiki-status" id="wiki-status"><div class="wiki-status-spinner"></div><span id="wiki-status-text"></span></div>';
      html += '<div class="wiki-actions">';
      if (this.currentSession) {
        html += `<button class="btn btn-sm btn-primary" onclick="App.wikiIngestCurrent()">Ingest Session</button>`;
      }
      html += `<button class="btn btn-sm" onclick="App.wikiIngestAll()">Ingest All</button>`;
      html += `<label class="wiki-llm-toggle" title="Use LLM for richer ingest (requires configured API key)"><input type="checkbox" id="wiki-llm-toggle" ${this._wikiUseLLM ? 'checked' : ''} onchange="App._wikiUseLLM=this.checked"> LLM</label>`;
      html += `<button class="btn btn-sm btn-ghost" onclick="App.wikiLint()">Lint</button>`;
      html += `<button class="btn btn-sm btn-ghost" onclick="App.wikiConsolidate()">Consolidate</button>`;
      html += '</div>';

      // Add entry
      html += '<div class="wiki-actions" id="wiki-add-row">';
      html += `<input class="wiki-search-input" id="wiki-add-input" placeholder="New page title..." style="flex:1" onkeydown="if(event.key==='Enter')App.wikiAddPage()">`;
      html += `<button class="btn btn-sm btn-ghost" onclick="App.wikiAddPage()">Add</button>`;
      html += '</div>';

      // Stats bar
      html += `<div style="display:flex;gap:12px;padding:8px 14px;font-size:11px;color:var(--text-muted)">`;
      html += `<span>${stats.pages} pages</span>`;
      html += `<span>${stats.links} links</span>`;
      html += `<span>${stats.timeline_entries} timeline entries</span>`;
      html += '</div>';

      // Page list (top 20)
      if (pages.length === 0) {
        if (isSearch) {
          html += '<div class="placeholder-text">No wiki pages match your search.</div>';
        } else {
          html += '<div class="placeholder-text">No wiki pages yet. Click "Ingest This Session" after a chat to start building your knowledge base.</div>';
        }
      } else {
        const typeLabels = { session_summary: 'Session', concept: 'Concept', entity: 'Entity', synthesis: 'Synthesis', index: 'Index' };
        const display = pages.slice(0, 20);
        display.forEach(p => {
          const label = typeLabels[p.page_type] || p.page_type;
          html += `<div class="wiki-page-item" onclick="App.wikiViewPage('${this.esc(p.slug)}')">
            <span class="wpi-icon" style="font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.05em;min-width:50px">${label}</span>
            <span class="wpi-title">${this.esc(p.title)}</span>
            <span class="wpi-meta">v${p.version}</span>
          </div>`;
        });
        if (pages.length > 20) {
          html += `<div style="padding:8px 14px;font-size:11px;color:var(--text-muted)">Showing top 20 of ${pages.length} pages${isSearch ? ' matching your search' : ''}</div>`;
        }
      }
      panel.innerHTML = html;

      // Focus search if user was typing
      if (isSearch) {
        const input = document.getElementById('wiki-search-input');
        if (input) { input.focus(); input.selectionStart = input.selectionEnd = input.value.length; }
      }
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },
  _wikiSearchTimeout: null,
  _wikiUseLLM: false,

  _wikiSetStatus(msg) {
    const bar = document.getElementById('wiki-status');
    const text = document.getElementById('wiki-status-text');
    if (bar) { bar.classList.add('active'); }
    if (text) { text.textContent = msg; }
  },
  _wikiClearStatus() {
    const bar = document.getElementById('wiki-status');
    if (bar) bar.classList.remove('active');
  },

  async wikiIngestCurrent() {
    if (!this.currentSession) return;
    const llm = this._wikiUseLLM;
    this._wikiSetStatus(llm ? 'LLM-ingesting session...' : 'Ingesting session...');
    try {
      const res = await fetch('/api/wiki/ingest', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ session_id: this.currentSession.id, use_llm: llm })
      });
      const data = await res.json();
      if (data.error) { this.toast(data.error, 'error'); return; }
      const label = data.llm_enhanced ? 'LLM-ingested' : 'Ingested';
      const regen = data.pages_regenerated ? `, ${data.pages_regenerated} regenerated` : '';
      this.toast(`${label}: ${data.pages_created} created, ${data.pages_updated} updated${regen}`, 'success');
      this.showWiki();
      this.refreshKG();
    } catch (e) { this.toast('Ingest failed: ' + e.message, 'error'); }
    finally { this._wikiClearStatus(); }
  },

  async wikiIngestAll() {
    const llm = this._wikiUseLLM;
    this._wikiSetStatus(llm ? 'LLM-ingesting all sessions...' : 'Ingesting all sessions...');
    try {
      const res = await fetch('/api/wiki/ingest-all', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ use_llm: llm })
      });
      const data = await res.json();
      this.toast(`Processed ${data.sessions_processed} sessions: ${data.total_pages_created} new, ${data.total_pages_updated} updated${data.llm_enhanced ? ' (LLM)' : ''}`, 'success');
      this.showWiki();
      this.refreshKG();
    } catch (e) { this.toast('Ingest failed: ' + e.message, 'error'); }
    finally { this._wikiClearStatus(); }
  },

  async wikiAddPage() {
    const input = document.getElementById('wiki-add-input');
    if (!input) return;
    const title = input.value.trim();
    if (!title) { this.toast('Enter a page title', 'info'); return; }
    this._wikiSetStatus('Creating page...');
    try {
      const res = await fetch('/api/wiki/add', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ title, page_type: 'concept' })
      });
      const data = await res.json();
      if (data.error) { this.toast(data.error, 'error'); return; }
      this.toast(`Created page: ${data.title}`, 'success');
      input.value = '';
      this.wikiViewPage(data.slug);
      this.refreshKG();
    } catch (e) { this.toast('Failed: ' + e.message, 'error'); }
    finally { this._wikiClearStatus(); }
  },

  async wikiViewPage(slug) {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      const res = await fetch(`/api/wiki/pages/${encodeURIComponent(slug)}`);
      if (!res.ok) { this.toast('Page not found', 'error'); this.showWiki(); return; }
      const page = await res.json();
      let html = '';
      html += `<div class="wiki-breadcrumb"><a onclick="App.showWiki()">Wiki</a> / ${this.esc(page.title)}</div>`;
      html += `<div class="wiki-page-view">${this._renderWikiMd(page.content)}</div>`;

      // Backlinks (R3.11): pages that link TO this page
      const links = page.links || {};
      const backlinks = links.inbound || [];
      if (backlinks.length > 0) {
        html += '<div class="info-section"><div class="info-section-title">Backlinks</div>';
        for (const l of backlinks) {
          html += `<div class="wiki-backlink-item" onclick="App.wikiViewPage('${this.esc(l.slug)}')">
            <span class="wbl-arrow">←</span>
            <span class="wbl-slug">${this.esc(l.slug)}</span>
            <span class="wbl-type">${this.esc(l.type)}</span>
          </div>`;
        }
        html += '</div>';
      }

      // Outbound links
      const outlinks = links.outbound || [];
      if (outlinks.length > 0) {
        html += '<div class="info-section"><div class="info-section-title">Links To</div>';
        for (const l of outlinks) {
          html += `<div class="wiki-backlink-item" onclick="App.wikiViewPage('${this.esc(l.slug)}')">
            <span class="wbl-arrow">→</span>
            <span class="wbl-slug">${this.esc(l.slug)}</span>
            <span class="wbl-type">${this.esc(l.type)}</span>
          </div>`;
        }
        html += '</div>';
      }

      // Metadata
      if (page.frontmatter) {
        const fm = page.frontmatter;
        html += '<div class="info-section"><div class="info-section-title">Metadata</div>';
        html += `<div style="font-size:11px;padding:4px 14px;color:var(--text-secondary)">`;
        if (fm.current_phase) html += `Phase: <span class="phase-badge ${fm.current_phase}">${fm.current_phase}</span> `;
        if (fm.total_sessions) html += `· ${fm.total_sessions} sessions `;
        if (fm.session_count) html += `· ${fm.session_count} appearances `;
        html += `· v${page.version}`;
        html += '</div></div>';
      }

      // Version history (R3.7)
      if (page.version > 1) {
        html += `<div class="info-section"><div class="info-section-title">Version History</div>`;
        html += `<div class="wiki-history-actions">`;
        html += `<button class="btn btn-sm btn-ghost" onclick="App.wikiViewHistory('${this.esc(slug)}')">Show all versions</button>`;
        html += `<button class="btn btn-sm btn-ghost" onclick="App.wikiViewDiff('${this.esc(slug)}', ${page.version - 1}, ${page.version})">Diff v${page.version - 1} → v${page.version}</button>`;
        html += `</div></div>`;
      }

      // Actions: Generate (LLM) for any content page
      if (page.page_type !== 'index' && page.page_type !== 'session_summary') {
        html += `<div class="info-section"><div class="info-section-title">Actions</div>`;
        html += `<div class="wiki-history-actions">`;
        html += `<button class="btn btn-sm btn-primary" onclick="App.wikiGenerate('${this.esc(slug)}')">Generate (LLM)</button>`;
        html += `<button class="btn btn-sm btn-ghost" onclick="App.wikiGenerate('${this.esc(slug)}', true)">Force Rewrite</button>`;
        html += `</div></div>`;
      }

      panel.innerHTML = html;
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },

  async wikiViewHistory(slug) {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      const res = await fetch(`/api/wiki/pages/${encodeURIComponent(slug)}/history`);
      const history = await res.json();
      let html = `<div class="wiki-breadcrumb"><a onclick="App.showWiki()">Wiki</a> / <a onclick="App.wikiViewPage('${this.esc(slug)}')">${this.esc(slug)}</a> / History</div>`;
      if (!history.length) {
        html += '<div class="placeholder-text">No version history.</div>';
      } else {
        html += '<div class="wiki-history-list">';
        for (let i = history.length - 1; i >= 0; i--) {
          const h = history[i];
          const isCurrent = i === history.length - 1;
          html += `<div class="wiki-history-item">
            <div class="whi-header">
              <span class="whi-version">v${h.version}</span>
              <span class="whi-hash">${h.content_hash}</span>
              <span class="whi-date">${(h.saved_at || '').substring(0, 16)}</span>
              ${isCurrent ? '<span class="whi-current">current</span>' : ''}
            </div>
            <div class="whi-actions">
              ${i > 0 ? `<a onclick="App.wikiViewDiff('${this.esc(slug)}', ${history[i-1].version}, ${h.version})">Diff with v${history[i-1].version}</a>` : ''}
            </div>
          </div>`;
        }
        html += '</div>';
      }
      panel.innerHTML = html;
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },

  async wikiViewDiff(slug, v1, v2) {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      const res = await fetch(`/api/wiki/pages/${encodeURIComponent(slug)}/diff?v1=${v1}&v2=${v2}`);
      const diff = await res.json();
      if (diff.error) { this.toast(diff.error, 'error'); return; }

      let html = `<div class="wiki-breadcrumb"><a onclick="App.showWiki()">Wiki</a> / <a onclick="App.wikiViewPage('${this.esc(slug)}')">${this.esc(slug)}</a> / Diff</div>`;
      html += `<div class="wiki-diff-header">
        <span>v${diff.v1} <span class="whi-hash">${diff.hash_v1}</span></span>
        <span>→</span>
        <span>v${diff.v2} <span class="whi-hash">${diff.hash_v2}</span></span>
        <span class="wiki-diff-stats">+${diff.stats.added} −${diff.stats.removed}</span>
      </div>`;

      html += '<div class="wiki-diff-body">';
      for (const line of diff.lines) {
        const cls = line.op === 'add' ? 'diff-add' : line.op === 'remove' ? 'diff-remove' : 'diff-equal';
        const prefix = line.op === 'add' ? '+' : line.op === 'remove' ? '−' : ' ';
        html += `<div class="diff-line ${cls}"><span class="diff-prefix">${prefix}</span>${this.esc(line.text)}</div>`;
      }
      html += '</div>';

      panel.innerHTML = html;
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },

  async wikiConsolidate() {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    try {
      // Load ignored pairs from localStorage
      const ignored = JSON.parse(localStorage.getItem('tm-consolidate-ignore') || '[]');
      const ignoreParam = ignored.length ? `?ignore=${encodeURIComponent(ignored.join(','))}` : '';
      const res = await fetch(`/api/wiki/related${ignoreParam}`);
      const groups = await res.json();
      let html = `<div class="wiki-breadcrumb"><a onclick="App.showWiki()">Wiki</a> / Consolidate</div>`;
      if (!groups.length) {
        html += '<div class="placeholder-text">No related concepts found to consolidate.</div>';
        if (ignored.length) {
          html += `<div style="padding:8px 14px"><button class="btn btn-sm btn-ghost" onclick="App.wikiClearIgnored()">Clear ${ignored.length} ignored pair(s)</button></div>`;
        }
      } else {
        html += `<div style="padding:8px 14px;font-size:12px;color:var(--text-secondary)">${groups.length} group(s) of related concepts${ignored.length ? ` (${ignored.length} ignored)` : ''}</div>`;
        for (const g of groups) {
          html += '<div class="wiki-consolidate-group">';
          html += `<div class="wcg-header">Shared: ${this.esc(g.shared_words.join(', '))}</div>`;
          for (const c of g.concepts) {
            html += `<div class="wcg-concept">
              <span class="wcg-title" onclick="App.wikiViewPage('${this.esc(c.slug)}')">${this.esc(c.title)}</span>
              <span class="wcg-meta">v${c.version}</span>
            </div>`;
          }
          if (g.concepts.length >= 2) {
            const primary = g.concepts[0].slug;
            const mergeList = g.concepts.slice(1).map(c => c.slug);
            const pairKey = [primary, ...mergeList].sort().join('|');
            html += `<div style="display:flex;gap:6px;margin:6px 0">`;
            html += `<button class="btn btn-sm btn-primary" onclick="App.wikiDoMerge('${this.esc(primary)}', ${JSON.stringify(mergeList).replace(/"/g, '&quot;')})">Merge into ${this.esc(g.concepts[0].title)}</button>`;
            html += `<button class="btn btn-sm btn-ghost" onclick="App.wikiIgnoreGroup('${this.esc(pairKey)}')">Ignore</button>`;
            html += '</div>';
          }
          html += '</div>';
        }
      }
      panel.innerHTML = html;
    } catch (e) { panel.innerHTML = `<div class="placeholder-text">Error: ${e.message}</div>`; }
  },

  wikiIgnoreGroup(pairKey) {
    const ignored = JSON.parse(localStorage.getItem('tm-consolidate-ignore') || '[]');
    if (!ignored.includes(pairKey)) {
      ignored.push(pairKey);
      localStorage.setItem('tm-consolidate-ignore', JSON.stringify(ignored));
    }
    this.toast('Group ignored', 'info');
    this.wikiConsolidate();
  },

  wikiClearIgnored() {
    localStorage.removeItem('tm-consolidate-ignore');
    this.toast('Ignore list cleared', 'success');
    this.wikiConsolidate();
  },

  async wikiDoMerge(primary, mergeList) {
    this._wikiSetStatus('Merging concepts...');
    try {
      const res = await fetch('/api/wiki/consolidate', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ primary, merge: mergeList })
      });
      const data = await res.json();
      if (data.error) { this.toast(data.error, 'error'); return; }
      this.toast(`Merged ${data.merged.length} page(s) into ${primary}`, 'success');
      this.refreshKG();
      this.wikiViewPage(primary);
    } catch (e) { this.toast('Merge failed: ' + e.message, 'error'); }
    finally { this._wikiClearStatus(); }
  },

  async wikiGenerate(slug, force = false) {
    this._wikiSetStatus(force ? 'Force rewriting page via LLM...' : 'Generating structured page via LLM...');
    try {
      const res = await fetch('/api/wiki/generate', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ slug, force })
      });
      const data = await res.json();
      if (data.error) { this.toast(data.error, 'error'); return; }
      if (data.no_change) {
        this.toast(`${data.title}: LLM says no changes needed`, 'info');
      } else {
        this.toast(`Generated wiki page for ${data.title}`, 'success');
        this.wikiViewPage(slug);
        this.refreshKG();
      }
    } catch (e) { this.toast('Generate failed: ' + e.message, 'error'); }
    finally { this._wikiClearStatus(); }
  },

  async wikiLint() {
    const panel = document.getElementById('right-content');
    panel.innerHTML = '<div class="skeleton skeleton-line" style="margin:16px"></div>';
    this._wikiSetStatus('Running lint checks...');
    try {
      const res = await fetch('/api/wiki/lint');
      const data = await res.json();
      let html = `<div class="wiki-breadcrumb"><a onclick="App.showWiki()">Wiki</a> / Lint Report</div>`;
      html += `<div style="padding:12px 14px;font-size:13px;color:var(--text-secondary)">${data.total_pages} pages, ${data.total_links} links</div>`;
      if (!data.issues.length) {
        html += '<div class="placeholder-text" style="padding:20px">No issues found</div>';
      } else {
        for (const issue of data.issues) {
          html += `<div class="wiki-lint-item">
            <span class="lint-type ${issue.type}">${issue.type}</span>
            <span style="margin-left:8px">${this.esc(issue.title || issue.slug || issue.to || '')}</span>
            ${issue.suggestion ? `<div style="font-size:11px;color:var(--text-muted);margin-top:2px">${this.esc(issue.suggestion)}</div>` : ''}
          </div>`;
        }
      }
      panel.innerHTML = html;
      this.toast(`Lint: ${data.issues.length} issues`, data.issues.length ? 'info' : 'success');
    } catch (e) { this.toast('Lint failed: ' + e.message, 'error'); }
    finally { this._wikiClearStatus(); }
  },

  _renderWikiMd(md) {
    if (!md) return '';
    let html = this.esc(md);
    // Headers
    html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
    html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');
    // Bold, italic, code
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
    // Wiki links [[slug|title]]
    html = html.replace(/\[\[([^|]+)\|([^\]]+)\]\]/g, '<a class="wiki-link" onclick="App.wikiViewPage(\'$1\')">$2</a>');
    html = html.replace(/\[\[([^\]]+)\]\]/g, '<a class="wiki-link" onclick="App.wikiViewPage(\'$1\')">$1</a>');
    // Lists
    html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
    html = html.replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>');
    // Blockquotes
    html = html.replace(/^&gt; (.+)$/gm, '<blockquote>$1</blockquote>');
    // Phase badges
    html = html.replace(/phase: (emerging|established|challenged|revised|deprecated|mentioned)/gi,
      (_, p) => `phase: <span class="phase-badge ${p.toLowerCase()}">${p}</span>`);
    // Line breaks (but not inside tags)
    html = html.replace(/\n/g, '<br>');
    // Clean up double <br> after block elements
    html = html.replace(/<\/(h[123]|ul|li|blockquote)><br>/g, '</$1>');
    return html;
  },
};

document.addEventListener('DOMContentLoaded', () => App.init());

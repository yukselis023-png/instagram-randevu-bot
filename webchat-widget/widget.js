/* DOEL AI Agent — Embeddable Web Chat Widget
 * Usage: <script src="https://api.doeldigital.com/webchat/widget.js?tenant=mybrand"></script>
 */
(function() {
    'use strict';

    // ── Config ─────────────────────────────────────────────────────
    var tenant = '__TENANT__';
    var apiBase = '__API_BASE__' || 'https://instagram-randevu-bot.onrender.com';
    var brandColor = '#1a1a2e';
    var logoUrl = '';

    // Parse query params
    var scripts = document.getElementsByTagName('script');
    for (var i = 0; i < scripts.length; i++) {
        var src = scripts[i].src || '';
        if (src.indexOf('widget.js') > -1) {
            var params = new URLSearchParams(src.split('?')[1] || '');
            if (params.get('tenant')) tenant = params.get('tenant');
            if (params.get('color')) brandColor = '#' + params.get('color');
            break;
        }
    }

    // ── State ──────────────────────────────────────────────────────
    var sessionId = localStorage.getItem('doel_chat_session_' + tenant);
    var isOpen = false;
    var bubbleEl, panelEl, messagesEl, inputEl, sendEl;

    // ── Inject styles ──────────────────────────────────────────────
    var style = document.createElement('style');
    style.textContent = `
        #doel-chat-bubble {
            position: fixed; bottom: 20px; right: 20px; z-index: 999999;
            width: 60px; height: 60px; border-radius: 50%;
            background: ${brandColor}; color: #fff;
            display: flex; align-items: center; justify-content: center;
            cursor: pointer; box-shadow: 0 4px 16px rgba(0,0,0,0.2);
            transition: transform 0.2s; font-size: 28px;
        }
        #doel-chat-bubble:hover { transform: scale(1.1); }
        #doel-chat-panel {
            position: fixed; bottom: 90px; right: 20px; z-index: 999999;
            width: 360px; height: 520px; background: #fff;
            border-radius: 16px; box-shadow: 0 8px 32px rgba(0,0,0,0.15);
            display: none; flex-direction: column; overflow: hidden;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        }
        #doel-chat-panel.open { display: flex; }
        #doel-chat-header {
            background: ${brandColor}; color: #fff; padding: 14px 18px;
            font-size: 15px; font-weight: 600; display: flex;
            justify-content: space-between; align-items: center;
        }
        #doel-chat-close { cursor: pointer; opacity: 0.8; font-size: 18px; }
        #doel-chat-messages {
            flex: 1; overflow-y: auto; padding: 14px; 
            background: #f5f5f5; display: flex; flex-direction: column;
            gap: 8px; font-size: 14px; line-height: 1.4;
        }
        .doel-msg { max-width: 80%; padding: 10px 14px; border-radius: 14px; }
        .doel-msg.user { align-self: flex-end; background: ${brandColor}; color: #fff; border-bottom-right-radius: 4px; }
        .doel-msg.bot { align-self: flex-start; background: #e8e8e8; color: #222; border-bottom-left-radius: 4px; }
        .doel-msg.typing { align-self: flex-start; background: transparent; color: #888; font-style: italic; }
        #doel-chat-input-area {
            display: flex; padding: 10px; border-top: 1px solid #e0e0e0;
            background: #fff;
        }
        #doel-chat-input {
            flex: 1; border: 1px solid #ddd; border-radius: 20px;
            padding: 10px 14px; font-size: 14px; outline: none;
        }
        #doel-chat-input:focus { border-color: ${brandColor}; }
        #doel-chat-send {
            background: ${brandColor}; color: #fff; border: none;
            width: 40px; height: 40px; border-radius: 50%; margin-left: 8px;
            cursor: pointer; font-size: 18px; display: flex;
            align-items: center; justify-content: center;
        }
        @media (max-width: 480px) {
            #doel-chat-panel { width: 100vw; height: 100vh; bottom: 0; right: 0; border-radius: 0; }
            #doel-chat-bubble { bottom: 20px; right: 20px; }
        }
    `;
    document.head.appendChild(style);

    // ── Create elements ─────────────────────────────────────────────
    bubbleEl = document.createElement('div');
    bubbleEl.id = 'doel-chat-bubble';
    bubbleEl.textContent = '💬';
    document.body.appendChild(bubbleEl);

    panelEl = document.createElement('div');
    panelEl.id = 'doel-chat-panel';
    panelEl.className = 'doel-chat-theme';
    panelEl.innerHTML = `
        <div id="doel-chat-header">
            <span>💬 Mesaj</span>
            <span id="doel-chat-close">✕</span>
        </div>
        <div id="doel-chat-messages">
            <div class="doel-msg bot">Merhaba! Size nasıl yardımcı olabilirim?</div>
        </div>
        <div id="doel-chat-input-area">
            <input id="doel-chat-input" type="text" placeholder="Mesajınızı yazın..." />
            <button id="doel-chat-send">➤</button>
        </div>
    `;
    document.body.appendChild(panelEl);

    // ── References ──────────────────────────────────────────────────
    messagesEl = document.getElementById('doel-chat-messages');
    inputEl = document.getElementById('doel-chat-input');
    sendEl = document.getElementById('doel-chat-send');
    var closeEl = document.getElementById('doel-chat-close');

    // ── Functions ───────────────────────────────────────────────────
    function addMessage(text, role) {
        var div = document.createElement('div');
        div.className = 'doel-msg ' + role;
        div.textContent = text;
        messagesEl.appendChild(div);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function showTyping() {
        var div = document.createElement('div');
        div.className = 'doel-msg typing';
        div.id = 'doel-typing';
        div.textContent = 'Yazıyor...';
        messagesEl.appendChild(div);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function hideTyping() {
        var el = document.getElementById('doel-typing');
        if (el) el.remove();
    }

    function sendMessage(text) {
        addMessage(text, 'user');
        showTyping();

        fetch(apiBase + '/api/channel/webchat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                session_id: sessionId,
                tenant: tenant,
                message: text,
            })
        })
        .then(function(r) { return r.json(); })
        .then(function(data) {
            hideTyping();
            if (data.ok && data.result && data.result.reply_text) {
                addMessage(data.result.reply_text, 'bot');
                if (data.result.session_id) {
                    sessionId = data.result.session_id;
                    localStorage.setItem('doel_chat_session_' + tenant, sessionId);
                }
            } else {
                addMessage('Bir hata oluştu. Lütfen tekrar deneyin.', 'bot');
            }
        })
        .catch(function(err) {
            hideTyping();
            addMessage('Bağlantı hatası. Lütfen daha sonra tekrar deneyin.', 'bot');
            console.error('DOEL Chat error:', err);
        });
    }

    // ── Event listeners ─────────────────────────────────────────────
    bubbleEl.addEventListener('click', function() {
        isOpen = !isOpen;
        panelEl.className = isOpen ? 'open' : '';
        bubbleEl.textContent = isOpen ? '✕' : '💬';
        if (isOpen) inputEl.focus();
    });

    closeEl.addEventListener('click', function() {
        isOpen = false;
        panelEl.className = '';
        bubbleEl.textContent = '💬';
    });

    sendEl.addEventListener('click', function() {
        var text = inputEl.value.trim();
        if (!text) return;
        inputEl.value = '';
        sendMessage(text);
    });

    inputEl.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            sendEl.click();
        }
    });

    console.log('DOEL AI Chat Widget loaded — tenant:', tenant);
})();

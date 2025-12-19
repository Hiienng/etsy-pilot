// ChatGPT Shared Conversation Loader
class ChatGPTLoader {
  constructor(shareUrl, containerId) {
    this.shareUrl = shareUrl;
    this.containerId = containerId;
    this.container = document.getElementById(containerId);
  }

  async load() {
    if (!this.container) {
      console.error(`Container ${this.containerId} not found`);
      return;
    }

    // Show loading state
    this.container.innerHTML = this.getLoadingHTML();

    try {
      // Fetch the shared conversation page
      const response = await fetch(this.shareUrl);
      const html = await response.text();

      // Parse and extract conversation
      const conversation = this.parseConversation(html);

      // Render conversation
      this.render(conversation);
    } catch (error) {
      console.error('Error loading ChatGPT conversation:', error);
      this.renderError();
    }
  }

  parseConversation(html) {
    // Create a temporary DOM to parse
    const parser = new DOMParser();
    const doc = parser.parseFromString(html, 'text/html');

    // Try to extract conversation data from various possible locations
    // ChatGPT shared links usually embed data in script tags or specific divs

    // For now, we'll use iframe embed as fallback
    // Note: ChatGPT shared conversations don't always allow iframe embedding
    return {
      title: this.extractTitle(doc),
      url: this.shareUrl,
      useIframe: true
    };
  }

  extractTitle(doc) {
    const titleElement = doc.querySelector('title') || doc.querySelector('h1');
    return titleElement ? titleElement.textContent : 'ChatGPT Conversation';
  }

  render(conversation) {
    if (conversation.useIframe) {
      // Use iframe embedding
      this.container.innerHTML = this.getIframeHTML(conversation);
    } else {
      // Render extracted messages
      this.container.innerHTML = this.getMessagesHTML(conversation);
    }
  }

  getLoadingHTML() {
    return `
      <div class="chatgpt-loading">
        <div class="spinner"></div>
        <p>Loading ChatGPT conversation...</p>
      </div>
    `;
  }

  getIframeHTML(conversation) {
    return `
      <div class="chatgpt-embed">
        <div class="chatgpt-header">
          <div class="chatgpt-icon">💬</div>
          <div class="chatgpt-info">
            <h3>ChatGPT Conversation</h3>
            <p>${conversation.title}</p>
          </div>
          <a href="${conversation.url}" target="_blank" class="chatgpt-open-btn">
            Open in ChatGPT ↗
          </a>
        </div>
        <div class="chatgpt-iframe-container">
          <iframe
            src="${conversation.url}"
            frameborder="0"
            width="100%"
            height="800px"
            sandbox="allow-scripts allow-same-origin"
            title="ChatGPT Conversation">
          </iframe>
          <div class="chatgpt-fallback">
            <p>⚠️ Không thể embed ChatGPT conversation trực tiếp.</p>
            <p>ChatGPT không cho phép embedding qua iframe vì security policy.</p>
            <a href="${conversation.url}" target="_blank" class="chatgpt-link-btn">
              📖 Xem ChatGPT Conversation (Tab mới)
            </a>
          </div>
        </div>
      </div>
    `;
  }

  getMessagesHTML(conversation) {
    let html = '<div class="chatgpt-messages">';

    conversation.messages?.forEach(msg => {
      const role = msg.role === 'user' ? 'user' : 'assistant';
      html += `
        <div class="chatgpt-message chatgpt-${role}">
          <div class="message-header">
            <span class="message-icon">${role === 'user' ? '👤' : '🤖'}</span>
            <span class="message-role">${role === 'user' ? 'You' : 'ChatGPT'}</span>
          </div>
          <div class="message-content">
            ${this.formatContent(msg.content)}
          </div>
        </div>
      `;
    });

    html += '</div>';
    return html;
  }

  formatContent(content) {
    // Basic markdown-like formatting
    return content
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.*?)\*/g, '<em>$1</em>')
      .replace(/`(.*?)`/g, '<code>$1</code>')
      .replace(/\n/g, '<br>');
  }

  renderError() {
    this.container.innerHTML = `
      <div class="chatgpt-error">
        <h3>⚠️ Không thể tải ChatGPT conversation</h3>
        <p>Do security policy, ChatGPT không cho phép embedding trực tiếp.</p>
        <a href="${this.shareUrl}" target="_blank" class="chatgpt-link-btn">
          📖 Xem ChatGPT Conversation (Tab mới)
        </a>
      </div>
    `;
  }
}

// Helper function to initialize ChatGPT loader
function loadChatGPTConversation(shareUrl, containerId) {
  const loader = new ChatGPTLoader(shareUrl, containerId);
  loader.load();
}

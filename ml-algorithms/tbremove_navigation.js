// Navigation System with Right-Click Context Menu
class NavigationSystem {
  constructor() {
    this.config = null;
    this.currentPage = this.getCurrentPageId();
    this.contextMenu = null;
  }

  async init() {
    try {
      await this.loadConfig();
      this.buildSidebar();
      this.buildContextMenu();
      this.setupEventListeners();
      this.highlightCurrentPage();
    } catch (error) {
      console.error('Navigation initialization error:', error);
    }
  }

  async loadConfig() {
    const response = await fetch('config.json');
    this.config = await response.json();
  }

  getCurrentPageId() {
    const path = window.location.pathname;
    const filename = path.split('/').pop();

    if (filename === 'index.html' || filename === '') {
      return 'home';
    }

    // Extract chapter id from filename (e.g., "chapter1.html" -> "chapter1")
    return filename.replace('.html', '');
  }

  buildSidebar() {
    const sidebar = document.getElementById('sidebar');
    if (!sidebar) return;

    let html = '<h3>📚 Table of Contents</h3>';

    this.config.parts.forEach((part, partIndex) => {
      const partNumber = partIndex + 1;
      html += `
        <div class="part">
          <div class="part-number">Part ${partNumber}</div>
          <div class="part-title">${part.title}</div>
          <ul>
      `;

      part.chapters.forEach(chapter => {
        const isActive = chapter.id === this.currentPage ? 'active' : '';
        html += `
          <li>
            <a href="${chapter.id}.html" class="${isActive}">
              <span class="chapter-badge">Ch ${chapter.chapterNumber}</span>
              <span class="chapter-text">${chapter.title}</span>
            </a>
          </li>
        `;
      });

      html += '</ul></div>';
    });

    sidebar.innerHTML = html;
  }

  buildContextMenu() {
    // Create context menu element
    const menu = document.createElement('div');
    menu.className = 'context-menu';
    menu.id = 'contextMenu';

    let html = '<div class="context-menu-header">📚 Navigate to Chapter</div>';

    this.config.parts.forEach(part => {
      html += `
        <div class="context-menu-part">
          <div class="context-menu-part-title">${part.title}</div>
      `;

      part.chapters.forEach(chapter => {
        const isActive = chapter.id === this.currentPage ? 'active' : '';
        html += `
          <div class="context-menu-item ${isActive}" data-url="${chapter.id}.html">
            <span class="chapter-badge">Ch ${chapter.chapterNumber}</span>
            <span>${chapter.title}</span>
          </div>
        `;
      });

      html += '</div>';
    });

    menu.innerHTML = html;
    document.body.appendChild(menu);
    this.contextMenu = menu;
  }

  setupEventListeners() {
    // Right-click to show context menu
    document.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      this.showContextMenu(e.pageX, e.pageY);
    });

    // Click anywhere to hide context menu
    document.addEventListener('click', () => {
      this.hideContextMenu();
    });

    // Context menu item clicks
    const menuItems = this.contextMenu.querySelectorAll('.context-menu-item');
    menuItems.forEach(item => {
      item.addEventListener('click', (e) => {
        e.stopPropagation();
        const url = item.getAttribute('data-url');
        if (url) {
          window.location.href = url;
        }
      });
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
      // Ctrl/Cmd + H for Home
      if ((e.ctrlKey || e.metaKey) && e.key === 'h') {
        e.preventDefault();
        window.location.href = 'index.html';
      }

      // Ctrl/Cmd + ← for Previous
      if ((e.ctrlKey || e.metaKey) && e.key === 'ArrowLeft') {
        e.preventDefault();
        this.navigatePrevious();
      }

      // Ctrl/Cmd + → for Next
      if ((e.ctrlKey || e.metaKey) && e.key === 'ArrowRight') {
        e.preventDefault();
        this.navigateNext();
      }
    });

    // Toggle sidebar on mobile
    const toggleBtn = document.getElementById('toggleSidebar');
    if (toggleBtn) {
      toggleBtn.addEventListener('click', () => {
        const sidebar = document.getElementById('sidebar');
        sidebar.classList.toggle('show');
      });
    }
  }

  showContextMenu(x, y) {
    this.contextMenu.style.left = `${x}px`;
    this.contextMenu.style.top = `${y}px`;
    this.contextMenu.classList.add('show');

    // Adjust position if menu goes off screen
    const rect = this.contextMenu.getBoundingClientRect();
    if (rect.right > window.innerWidth) {
      this.contextMenu.style.left = `${x - rect.width}px`;
    }
    if (rect.bottom > window.innerHeight) {
      this.contextMenu.style.top = `${y - rect.height}px`;
    }
  }

  hideContextMenu() {
    this.contextMenu.classList.remove('show');
  }

  highlightCurrentPage() {
    // Highlight in sidebar
    const sidebarLinks = document.querySelectorAll('.sidebar a');
    sidebarLinks.forEach(link => {
      if (link.getAttribute('href') === `${this.currentPage}.html`) {
        link.classList.add('active');
      }
    });
  }

  getAllChapters() {
    const chapters = [];
    this.config.parts.forEach(part => {
      part.chapters.forEach(chapter => {
        chapters.push(chapter);
      });
    });
    return chapters;
  }

  getCurrentChapterIndex() {
    const chapters = this.getAllChapters();
    return chapters.findIndex(ch => ch.id === this.currentPage);
  }

  navigatePrevious() {
    const chapters = this.getAllChapters();
    const currentIndex = this.getCurrentChapterIndex();

    if (currentIndex > 0) {
      window.location.href = `${chapters[currentIndex - 1].id}.html`;
    } else {
      window.location.href = 'index.html';
    }
  }

  navigateNext() {
    const chapters = this.getAllChapters();
    const currentIndex = this.getCurrentChapterIndex();

    if (currentIndex >= 0 && currentIndex < chapters.length - 1) {
      window.location.href = `${chapters[currentIndex + 1].id}.html`;
    }
  }

  getNavigationInfo() {
    const chapters = this.getAllChapters();
    const currentIndex = this.getCurrentChapterIndex();

    return {
      hasPrevious: currentIndex > 0 || this.currentPage !== 'home',
      hasNext: currentIndex >= 0 && currentIndex < chapters.length - 1,
      previous: currentIndex > 0 ? chapters[currentIndex - 1] : null,
      next: currentIndex >= 0 && currentIndex < chapters.length - 1 ? chapters[currentIndex + 1] : null
    };
  }
}

// Initialize navigation when DOM is ready
let navSystem;
document.addEventListener('DOMContentLoaded', async () => {
  navSystem = new NavigationSystem();
  await navSystem.init();

  // Build page navigation buttons if on chapter page
  const pageNav = document.getElementById('pageNavigation');
  if (pageNav) {
    const navInfo = navSystem.getNavigationInfo();

    let html = '';

    if (navInfo.hasPrevious) {
      const prevUrl = navInfo.previous ? `${navInfo.previous.id}.html` : 'index.html';
      const prevText = navInfo.previous ? `← Chapter ${navInfo.previous.chapterNumber}` : '← Home';
      html += `<a href="${prevUrl}" class="nav-button">
        ${prevText}
      </a>`;
    } else {
      html += '<span class="nav-button disabled">← Previous</span>';
    }

    html += '<a href="index.html" class="nav-button">🏠 Home</a>';

    if (navInfo.hasNext) {
      html += `<a href="${navInfo.next.id}.html" class="nav-button">
        Chapter ${navInfo.next.chapterNumber} →
      </a>`;
    } else {
      html += '<span class="nav-button disabled">Next →</span>';
    }

    pageNav.innerHTML = html;
  }
});

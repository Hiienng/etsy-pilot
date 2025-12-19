// MathJax Configuration for Beautiful Math Rendering

window.MathJax = {
  tex: {
    inlineMath: [['$', '$'], ['\\(', '\\)']],
    displayMath: [['$$', '$$'], ['\\[', '\\]']],
    processEscapes: true,
    processEnvironments: true,
    tags: 'ams',
    packages: {'[+]': ['ams', 'newcommand', 'configmacros']}
  },
  svg: {
    fontCache: 'global',
    scale: 1.2  // Make math formulas 20% larger
  },
  startup: {
    pageReady: () => {
      return MathJax.startup.defaultPageReady().then(() => {
        console.log('MathJax loaded and ready');
      });
    }
  },
  options: {
    renderActions: {
      addMenu: []  // Disable right-click menu
    }
  }
};

// Auto-convert simple text formulas to LaTeX
document.addEventListener('DOMContentLoaded', () => {
  setTimeout(() => {
    convertTextToLaTeX();
  }, 500);
});

function convertTextToLaTeX() {
  // Find all code blocks and divs with monospace font
  const mathElements = document.querySelectorAll('div[style*="monospace"], code');

  mathElements.forEach(el => {
    let text = el.textContent;

    // Skip if already has LaTeX delimiters
    if (text.includes('$') || text.includes('\\(')) return;

    // Convert common patterns to LaTeX
    text = text
      // Greek letters
      .replace(/\bmu\b/g, '$\\mu$')
      .replace(/\bsigma\b/g, '$\\sigma$')
      .replace(/\blambda\b/g, '$\\lambda$')
      .replace(/\btheta\b/g, '$\\theta$')
      .replace(/\balpha\b/g, '$\\alpha$')
      .replace(/\bbeta\b/g, '$\\beta$')
      .replace(/\brho\b/g, '$\\rho$')

      // Superscripts (simple cases)
      .replace(/\^2/g, '$^2$')
      .replace(/\^(\d+)/g, '^{$1}')

      // Subscripts
      .replace(/_(\w+)/g, '_{$1}')

      // Special symbols
      .replace(/<=>/g, '$\\Leftrightarrow$')
      .replace(/<=/g, '$\\leq$')
      .replace(/>=/g, '$\\geq$')
      .replace(/!=/g, '$\\neq$')
      .replace(/\binfinity\b/g, '$\\infty$')
      .replace(/∞/g, '$\\infty$')
      .replace(/∫/g, '$\\int$')
      .replace(/Σ/g, '$\\sum$')
      .replace(/√/g, '$\\sqrt{~}$')
      .replace(/π/g, '$\\pi$');

    el.innerHTML = text;
  });

  // Trigger MathJax to re-render
  if (window.MathJax && window.MathJax.typesetPromise) {
    MathJax.typesetPromise();
  }
}

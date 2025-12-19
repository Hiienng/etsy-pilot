// Enhanced Navigation with Collapsible Parts
// Add this script after navigation.js loads

document.addEventListener('DOMContentLoaded', () => {
  // Wait for navigation to be built
  setTimeout(() => {
    makePartsCollapsible();
  }, 200);
});

function makePartsCollapsible() {
  const parts = document.querySelectorAll('.sidebar .part');

  parts.forEach((part, index) => {
    // Wrap part info in clickable header
    const partNumber = part.querySelector('.part-number');
    const partTitle = part.querySelector('.part-title');
    const chapters = part.querySelector('ul');

    if (!partNumber || !partTitle || !chapters) return;

    // Create wrapper structure
    const header = document.createElement('div');
    header.className = 'part-header';

    const info = document.createElement('div');
    info.className = 'part-info';

    const toggle = document.createElement('span');
    toggle.className = 'part-toggle';
    toggle.innerHTML = '▼';

    // Move elements
    info.appendChild(partNumber);
    info.appendChild(partTitle);
    header.appendChild(info);
    header.appendChild(toggle);

    // Wrap chapters
    const chaptersWrapper = document.createElement('div');
    chaptersWrapper.className = 'part-chapters';
    chaptersWrapper.appendChild(chapters);

    // Clear part and rebuild
    part.innerHTML = '';
    part.appendChild(header);
    part.appendChild(chaptersWrapper);

    // Load collapsed state from localStorage
    const storageKey = `part-${index}-collapsed`;
    const isCollapsed = localStorage.getItem(storageKey) === 'true';

    if (isCollapsed) {
      chaptersWrapper.classList.add('collapsed');
      toggle.classList.add('collapsed');
    }

    // Toggle on click
    header.addEventListener('click', (e) => {
      e.stopPropagation();
      const isNowCollapsed = chaptersWrapper.classList.toggle('collapsed');
      toggle.classList.toggle('collapsed');

      // Save state
      localStorage.setItem(storageKey, isNowCollapsed);
    });
  });
}

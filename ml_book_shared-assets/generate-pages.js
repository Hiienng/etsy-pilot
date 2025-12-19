// Node.js script to generate chapter HTML pages from source files
// Usage: node generate-pages.js

const fs = require('fs');
const path = require('path');

// Read config
const config = JSON.parse(fs.readFileSync('config.json', 'utf8'));

// Template for chapter pages
const getChapterTemplate = (chapterNumber, chapterTitle, contentHtml) => `<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Chapter ${chapterNumber}: ${chapterTitle} - ML Mathematics</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>

<!-- Header -->
<div class="header">
  <h1>Machine Learning Mathematics</h1>
  <p>Complete Guide - Vietnamese Edition</p>
</div>

<!-- Sidebar Navigation -->
<div class="sidebar" id="sidebar">
  <!-- Will be populated by navigation.js -->
</div>

<!-- Toggle Sidebar Button (Mobile) -->
<button class="toggle-sidebar" id="toggleSidebar">☰ Menu</button>

<!-- Main Content -->
<div class="main-content">
  <div class="content-wrapper">

    <!-- Chapter Content -->
    <h1><span class="chapter-number">Chapter ${chapterNumber}</span>${chapterTitle}</h1>

    ${contentHtml}

    <!-- Page Navigation -->
    <div class="page-navigation" id="pageNavigation">
      <!-- Will be populated by navigation.js -->
    </div>

  </div>
</div>

<script src="navigation.js"></script>
</body>
</html>
`;

// Process each chapter
config.parts.forEach(part => {
  part.chapters.forEach(chapter => {
    try {
      console.log(`Processing ${chapter.file}...`);

      // Read source HTML file
      const sourceContent = fs.readFileSync(chapter.file, 'utf8');

      // Extract body content
      const bodyMatch = sourceContent.match(/<body[^>]*>([\s\S]*)<\/body>/i);
      if (!bodyMatch) {
        console.error(`  ❌ Could not find <body> in ${chapter.file}`);
        return;
      }

      let bodyContent = bodyMatch[1].trim();

      // Remove the first <h1> tag if it exists (we'll add it in template)
      bodyContent = bodyContent.replace(/<h1[^>]*>.*?<\/h1>/i, '').trim();

      // Generate chapter page
      const chapterHtml = getChapterTemplate(
        chapter.chapterNumber,
        chapter.title,
        bodyContent
      );

      // Write to chapter file
      const outputFile = `${chapter.id}.html`;
      fs.writeFileSync(outputFile, chapterHtml, 'utf8');

      console.log(`  ✓ Generated ${outputFile}`);

    } catch (error) {
      console.error(`  ❌ Error processing ${chapter.file}:`, error.message);
    }
  });
});

console.log('\n✅ All chapter pages generated!');
console.log('📖 Open index.html to start reading.');

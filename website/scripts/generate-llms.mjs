import { readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const docsDir = resolve(root, 'src/content/docs');
const distDir = resolve(root, 'dist');
const BASE_URL = 'https://anyserve.github.io/anyserve';

function walk(dir) {
  const entries = readdirSync(dir).sort();
  const files = [];
  for (const entry of entries) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      files.push(...walk(full));
    } else if (entry.endsWith('.mdx') || entry.endsWith('.md')) {
      files.push(full);
    }
  }
  return files;
}

function parseDoc(filePath) {
  const raw = readFileSync(filePath, 'utf8');
  const match = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  const frontmatter = match ? match[1] : '';
  const body = (match ? match[2] : raw)
    .replace(/^import\s+.*$/gm, '')
    .replace(/<CardGrid[^>]*>|<\/CardGrid>/g, '')
    .replace(/<Card\s+title="([^"]+)"[^>]*>/g, '### $1\n')
    .replace(/<\/Card>/g, '')
    .replace(/<Tabs[^>]*>|<\/Tabs>/g, '')
    .replace(/<TabItem\s+label="([^"]+)"[^>]*>/g, '#### $1\n')
    .replace(/<\/TabItem>/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  const titleMatch = frontmatter.match(/^title:\s*(.+)$/m);
  const descMatch = frontmatter.match(/^description:\s*(.+)$/m);
  const title = titleMatch ? titleMatch[1].trim().replace(/^["']|["']$/g, '') : 'AnyServe';
  const description = descMatch ? descMatch[1].trim().replace(/^["']|["']$/g, '') : '';

  const rel = relative(docsDir, filePath).replace(/\\/g, '/').replace(/\.(mdx|md)$/, '');
  const slug = rel === 'index' ? '' : `${rel}/`;
  return { title, description, url: `${BASE_URL}/${slug}`, body };
}

const docs = walk(docsDir).map(parseDoc);

const indexLines = [
  '# AnyServe',
  '',
  '> Call the GPU under your desk from anywhere. Outbound-only execution and scheduling control plane.',
  '',
  `- Repository: https://github.com/anyserve/anyserve`,
  `- Live Demo: https://anyserve.arcosx.org`,
  `- Install (macOS/Linux): curl -fsSL ${BASE_URL}/install.sh | sh`,
  `- Install (Windows): irm ${BASE_URL}/install.ps1 | iex`,
  '',
  '## Docs',
  '',
  ...docs.map((d) => `- [${d.title}](${d.url})${d.description ? `: ${d.description}` : ''}`),
  '',
];

const fullLines = [
  '# AnyServe — Full Documentation',
  '',
  ...docs.flatMap((d) => [`## ${d.title}`, `Source: ${d.url}`, '', d.body, '', '---', '']),
];

writeFileSync(join(distDir, 'llms.txt'), indexLines.join('\n'), 'utf8');
writeFileSync(join(distDir, 'llms-full.txt'), fullLines.join('\n'), 'utf8');
console.log(`Generated llms.txt and llms-full.txt (${docs.length} pages).`);

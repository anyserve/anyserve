import { copyFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = resolve(root, '..');
const publicDir = resolve(root, 'public');

mkdirSync(publicDir, { recursive: true });

for (const name of ['install.sh', 'install.ps1']) {
  const src = resolve(repoRoot, 'scripts', name);
  const dst = resolve(publicDir, name);
  copyFileSync(src, dst);
}

console.log('Synced installer scripts into website/public/.');

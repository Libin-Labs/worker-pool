import { copyFile, mkdir, writeFile } from 'node:fs/promises';

await mkdir(new URL('../dist/cjs/', import.meta.url), { recursive: true });
await writeFile(new URL('../dist/cjs/package.json', import.meta.url), '{"type":"commonjs"}\n');
await copyFile(
  new URL('../dist/types/index.d.ts', import.meta.url),
  new URL('../dist/types/index.d.mts', import.meta.url),
);
await copyFile(
  new URL('../dist/types/index.d.ts', import.meta.url),
  new URL('../dist/types/index.d.cts', import.meta.url),
);

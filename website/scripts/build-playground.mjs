import { cpSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const here = dirname(fileURLToPath(import.meta.url));
const websiteRoot = resolve(here, '..');
const repoRoot = resolve(websiteRoot, '..');
const outDir = join(websiteRoot, 'public', 'playground');

mkdirSync(outDir, { recursive: true });

const env = {
	...process.env,
	GOOS: 'js',
	GOARCH: 'wasm',
	GOCACHE: process.env.GOCACHE || '/tmp/t4-gocache',
};

execFileSync('go', ['build', '-o', join(outDir, 't4play.wasm'), '.'], {
	cwd: join(repoRoot, 'cmd', 'playground'),
	env,
	stdio: 'inherit',
});

const goroot = execFileSync('go', ['env', 'GOROOT'], {
	cwd: repoRoot,
	encoding: 'utf8',
}).trim();

cpSync(join(goroot, 'lib', 'wasm', 'wasm_exec.js'), join(outDir, 'wasm_exec.js'));

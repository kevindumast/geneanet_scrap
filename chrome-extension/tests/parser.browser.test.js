// Test du parseur HTML dans un vrai Chromium (DOMParser), via Playwright.
// Ignoré si Playwright n'est pas installé (npm i -D playwright && npx playwright install chromium).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createRequire } from 'node:module';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let chromium;
try {
  ({ chromium } = createRequire(import.meta.url)('playwright'));
} catch {
  /* Playwright absent */
}

test('lecture du tableau d’ascendance', { skip: !chromium && 'Playwright non installé' }, async () => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.route('http://ext.test/**', async (route) => {
      const file = path.join(root, new URL(route.request().url()).pathname);
      route.fulfill({ body: await readFile(file), contentType: file.endsWith('.js') ? 'text/javascript' : 'text/html' });
    });
    await page.goto('http://ext.test/tests/fixtures/ascendance.html');
    const html = await readFile(path.join(root, 'tests/fixtures/ascendance.html'), 'utf8');
    const persons = await page.evaluate(async (html) => {
      const { parseAncestorPage } = await import('/lib/geneanet.js');
      return parseAncestorPage(html, 'https://gw.geneanet.org/124559?lang=fr&m=A&p=arthur+victor&n=dumast');
    }, html);

    assert.deepEqual(persons.map((p) => p.sosa), [1, 2, 3, 4, 6, 2048]); // "? ?" ignoré, doublon ignoré
    const by = Object.fromEntries(persons.map((p) => [p.sosa, p]));
    assert.equal(by[1].given, 'Arthur Victor');
    assert.equal(by[1].surname, 'DUMAST');
    assert.equal(by[1].sex, 'U');
    assert.equal(by[1].deathPlace, 'Tours, 37000, Indre et Loire');
    assert.equal(by[1].url, 'https://gw.geneanet.org/124559?lang=fr&p=arthur+victor&n=dumast');
    assert.equal(by[2].occupation, 'Cultivateur');
    assert.equal(by[2].marriageDate, '28/09/1914');
    assert.equal(by[3].given, 'Gabrielle');
    assert.equal(by[3].surname, 'LE COQ');
    assert.equal(by[3].sex, 'F');
    assert.equal(by[4].title, 'comte');
    assert.equal(by[4].surname, 'DUMAST');
    assert.equal(by[6].birthDate, 'avant 1870'); // première occurrence conservée
    assert.equal(by[2048].surname, 'ANCIEN');
  } finally {
    await browser.close();
  }
});

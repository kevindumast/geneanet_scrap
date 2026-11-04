import { parseGeneanetUrl, buildExportUrl, parseAncestorPage, treeName } from './lib/geneanet.js';
import { buildGedcom, buildCsv, buildJson } from './lib/exporters.js';

const $ = (id) => document.getElementById(id);
const els = {
  form: $('form'),
  url: $('url'),
  tabHint: $('tabHint'),
  generations: $('generations'),
  exactUrl: $('exactUrl'),
  convertDates: $('convertDates'),
  fetch: $('fetch'),
  status: $('status'),
  result: $('result'),
  previewBody: $('previewBody'),
};

let state = { persons: null, treeUrl: '', name: '', geneanetTabId: null };

function setStatus(text, kind = '') {
  els.status.textContent = text;
  els.status.className = `status ${kind}`;
}

const isGeneanet = (url) => {
  try {
    return /(^|\.)geneanet\.org$/i.test(new URL(url).hostname);
  } catch {
    return false;
  }
};

async function init() {
  const saved = await chrome.storage.local.get(['generations', 'convertDates', 'exactUrl']).catch(() => ({}));
  if (saved.generations) els.generations.value = saved.generations;
  if (typeof saved.convertDates === 'boolean') els.convertDates.checked = saved.convertDates;
  if (typeof saved.exactUrl === 'boolean') els.exactUrl.checked = saved.exactUrl;

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (tab?.url && isGeneanet(tab.url)) {
    state.geneanetTabId = tab.id;
    els.url.value = tab.url;
    els.tabHint.textContent = "URL de l'onglet Geneanet courant (modifiable).";
  } else {
    els.tabHint.textContent = "Astuce : ouvre la fiche Geneanet de la personne de départ, puis clique sur l'extension.";
  }
}

/**
 * Télécharge la page d'export. On passe de préférence par l'onglet Geneanet ouvert :
 * la requête part alors de la page elle-même, avec la session de l'utilisateur
 * (arbres réservés aux amis, vérification anti-robot déjà passée…).
 */
async function fetchHtml(url) {
  if (state.geneanetTabId !== null) {
    try {
      const [injection] = await chrome.scripting.executeScript({
        target: { tabId: state.geneanetTabId },
        args: [url],
        func: async (u) => {
          try {
            const r = await fetch(u, { credentials: 'include' });
            if (!r.ok) return { error: `HTTP ${r.status}` };
            return { html: await r.text(), finalUrl: r.url };
          } catch (e) {
            return { error: String(e) };
          }
        },
      });
      const res = injection?.result;
      if (res?.html) return res;
      if (res?.error) console.warn('Échec du téléchargement depuis l’onglet :', res.error);
    } catch (e) {
      console.warn('Injection impossible, téléchargement depuis l’extension :', e);
    }
  }
  const r = await fetch(url, { credentials: 'include' });
  if (!r.ok) throw new Error(`Geneanet a répondu HTTP ${r.status}.`);
  return { html: await r.text(), finalUrl: r.url };
}

function renderPreview(persons) {
  els.previewBody.replaceChildren(
    ...persons.slice(0, 200).map((p) => {
      const tr = document.createElement('tr');
      for (const text of [p.sosa, `${p.given} ${p.surname}`.trim(), [p.birthDate, p.birthPlace].filter(Boolean).join(' — ')]) {
        const td = document.createElement('td');
        td.textContent = text;
        tr.append(td);
      }
      return tr;
    })
  );
}

els.form.addEventListener('submit', async (event) => {
  event.preventDefault();
  els.result.hidden = true;
  state.persons = null;
  const inputUrl = els.url.value.trim();
  const generations = Math.min(100, Math.max(1, parseInt(els.generations.value, 10) || 10));
  chrome.storage.local.set({
    generations,
    convertDates: els.convertDates.checked,
    exactUrl: els.exactUrl.checked,
  });

  let exportUrl;
  try {
    const info = parseGeneanetUrl(inputUrl);
    state.name = treeName(info);
    exportUrl = els.exactUrl.checked ? inputUrl : buildExportUrl(info, { generations });
  } catch (e) {
    setStatus(e.message, 'error');
    return;
  }

  els.fetch.disabled = true;
  setStatus(`Téléchargement de l'ascendance (${generations} générations)…`);
  try {
    const { html, finalUrl } = await fetchHtml(exportUrl);
    setStatus('Lecture du tableau…');
    const persons = parseAncestorPage(html, finalUrl || exportUrl);
    state.persons = persons;
    state.treeUrl = inputUrl;
    renderPreview(persons);
    els.result.hidden = false;
    setStatus(`${persons.length} personnes trouvées. Choisis le format à télécharger.`, 'ok');
  } catch (e) {
    console.error(e);
    setStatus(e.message || String(e), 'error');
  } finally {
    els.fetch.disabled = false;
  }
});

const FORMATS = {
  ged: { mime: 'text/plain', build: (p) => buildGedcom(p, { treeUrl: state.treeUrl, convertDates: els.convertDates.checked }) },
  csv: { mime: 'text/csv', build: (p) => buildCsv(p) },
  json: { mime: 'application/json', build: (p) => buildJson(p, { treeUrl: state.treeUrl }) },
};

const safeFileName = (s) => s.replace(/[\\/:*?"<>|]+/g, '_').trim() || 'arbre';

els.result.addEventListener('click', async (event) => {
  const format = event.target.closest('button[data-format]')?.dataset.format;
  if (!format || !state.persons) return;
  const { mime, build } = FORMATS[format];
  const blob = new Blob([build(state.persons)], { type: `${mime};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const date = new Date().toISOString().slice(0, 10);
  try {
    await chrome.downloads.download({
      url,
      filename: `arbres/${date}__${safeFileName(state.name)}.${format}`,
      saveAs: true,
    });
  } catch (e) {
    setStatus(`Téléchargement impossible : ${e.message}`, 'error');
  } finally {
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
  }
});

init();

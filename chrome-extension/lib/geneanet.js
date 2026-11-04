// Construction de l'URL d'export et lecture du tableau d'ascendance Geneanet.
// Équivalent JavaScript de la classe Url_Data et de
// get_dataframe_arbre_by_beautifoul_soup_url() du script run.py.

// Paramètres ajoutés à l'URL pour obtenir la liste d'ascendance complète
// (mêmes valeurs que Url_Data.parameters dans run.py).
export const EXPORT_PARAMS = {
  m: 'A', // mode ascendance
  t: 'Z', // affichage en tableau
  sosab: '10',
  ocz: '0',
  color: 'r',
  birth: 'on',
  birth_place: 'on',
  marr: 'on',
  marr_date: 'on',
  marr_place: 'on',
  death: 'on',
  death_place: 'on',
  occu: 'on',
  gen: 'on',
  repeat: 'on',
};

/** Lit une URL d'arbre Geneanet : base, prénom (p), nom (n), occurrence (oc). */
export function parseGeneanetUrl(input) {
  let url;
  try {
    url = new URL(String(input).trim());
  } catch {
    throw new Error("URL invalide. Colle l'adresse d'une fiche de l'arbre Geneanet.");
  }
  if (!/(^|\.)geneanet\.org$/i.test(url.hostname)) {
    throw new Error("Ce n'est pas une URL Geneanet (gw.geneanet.org).");
  }
  const base = url.pathname.split('/').filter(Boolean)[0];
  const p = url.searchParams.get('p');
  const n = url.searchParams.get('n');
  if (!base || !p || !n) {
    throw new Error(
      "L'URL doit pointer vers une personne de l'arbre (paramètres p= et n= présents). " +
        'Ouvre la fiche de la personne de départ puis réessaie.'
    );
  }
  return {
    origin: url.origin,
    base,
    p,
    n,
    oc: url.searchParams.get('oc') || '0',
  };
}

/** Nom de l'arbre comme dans run.py : "<nom>_<prénom>". */
export function treeName(info) {
  return `${info.n}_${info.p}`.replace(/\+/g, ' ');
}

/** URL d'export ascendance (Url_Data.formater_url dans run.py). */
export function buildExportUrl(input, { generations = 10 } = {}) {
  const info = typeof input === 'string' ? parseGeneanetUrl(input) : input;
  const url = new URL(`${info.origin}/${encodeURIComponent(info.base)}`);
  url.searchParams.set('lang', 'fr'); // les en-têtes du tableau sont lus en français
  url.searchParams.set('p', info.p);
  url.searchParams.set('n', info.n);
  if (info.oc && info.oc !== '0') url.searchParams.set('oc', info.oc);
  for (const [key, value] of Object.entries(EXPORT_PARAMS)) url.searchParams.set(key, value);
  url.searchParams.set('v', String(generations));
  return url.toString();
}

const normalize = (s) =>
  String(s || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();

const cleanText = (s) => String(s || '').replace(/ /g, ' ').replace(/\s+/g, ' ').trim();

// Associe chaque champ à une règle sur l'en-tête de colonne (normalisé, sans accents).
const COLUMN_RULES = {
  sosa: (h) => h.includes('sosa'),
  person: (h) => h.includes('personne'),
  birthDate: (h) => h.includes('naissance') && h.includes('date'),
  birthPlace: (h) => h.includes('naissance') && h.includes('lieu'),
  marriageDate: (h) => (h.includes('union') || h.includes('mariage')) && h.includes('date'),
  marriagePlace: (h) => (h.includes('union') || h.includes('mariage')) && h.includes('lieu'),
  deathDate: (h) => h.includes('deces') && h.includes('date'),
  deathPlace: (h) => h.includes('deces') && h.includes('lieu'),
  occupation: (h) => h.includes('profession'),
};

// Développe les cellules d'une ligne en tenant compte des colspan.
function expandCells(row) {
  const cells = [];
  for (const cell of row.cells) {
    const span = Math.max(1, parseInt(cell.getAttribute('colspan') || '1', 10) || 1);
    for (let i = 0; i < span; i++) cells.push(cell);
  }
  return cells;
}

function titleCase(s) {
  return s.toLowerCase().replace(/(^|[\s-])(\p{L})/gu, (m, sep, c) => sep + c.toUpperCase());
}

/**
 * Sépare "Prénoms NOM, titre" en prénom / nom / titre.
 * Le nombre de mots du nom est déduit du paramètre n= du lien (gère "LE COQ").
 */
export function splitName(text, href, pageUrl) {
  const [namePart, ...rest] = cleanText(text).split(',');
  const title = rest.join(',').trim();
  let pParam = '';
  let nParam = '';
  if (href) {
    try {
      const u = new URL(href, pageUrl || 'https://gw.geneanet.org/');
      pParam = u.searchParams.get('p') || '';
      nParam = u.searchParams.get('n') || '';
    } catch {
      /* lien illisible : on se contente du texte */
    }
  }
  const tokens = namePart.trim().split(' ').filter(Boolean);
  let given = '';
  let surname = '';
  const surnameWords = nParam ? nParam.trim().split(/\s+/).length : 0;
  if (surnameWords && tokens.length > surnameWords) {
    surname = tokens.slice(-surnameWords).join(' ');
    given = tokens.slice(0, -surnameWords).join(' ');
  } else {
    // Repli : le nom est la suite de mots en majuscules en fin de chaîne.
    let i = tokens.length;
    while (i > 0 && tokens[i - 1] === tokens[i - 1].toUpperCase() && /\p{L}/u.test(tokens[i - 1])) i--;
    if (i === tokens.length || i === 0) {
      surname = nParam ? nParam.toUpperCase() : tokens.slice(-1).join(' ');
      given = pParam ? titleCase(pParam) : tokens.slice(0, -1).join(' ');
    } else {
      surname = tokens.slice(i).join(' ');
      given = tokens.slice(0, i).join(' ');
    }
  }
  return { given, surname, title };
}

/** Trouve le tableau d'ascendance (celui dont une ligne d'en-tête contient "Sosa"). */
function findAncestorTable(doc) {
  for (const table of doc.querySelectorAll('table')) {
    for (const row of table.rows) {
      const headers = expandCells(row).map((c) => normalize(c.textContent));
      if (headers.some(COLUMN_RULES.sosa) && headers.some(COLUMN_RULES.person)) {
        return { table, headerRow: row, headers };
      }
    }
  }
  return null;
}

/**
 * Lit la page d'ascendance et renvoie la liste des personnes, une par numéro Sosa.
 * @param {string|Document} html  HTML de la page (ou Document déjà parsé)
 * @param {string} pageUrl        URL de la page (pour résoudre les liens relatifs)
 */
export function parseAncestorPage(html, pageUrl) {
  const doc = typeof html === 'string' ? new DOMParser().parseFromString(html, 'text/html') : html;
  const found = findAncestorTable(doc);
  if (!found) {
    throw new Error(
      "Tableau d'ascendance introuvable dans la page. L'arbre est peut-être privé, " +
        'ou Geneanet demande une connexion / une vérification anti-robot : ouvre l’arbre dans un onglet puis relance.'
    );
  }
  const { table, headerRow, headers } = found;
  const col = {};
  for (const [field, rule] of Object.entries(COLUMN_RULES)) {
    const index = headers.findIndex(rule);
    if (index >= 0) col[field] = index;
  }

  const persons = new Map();
  for (const row of table.rows) {
    if (row === headerRow) continue;
    const cells = expandCells(row);
    if (row.cells.length < 2) continue; // lignes "Génération N"
    const sosaText = cleanText(cells[col.sosa]?.textContent).replace(/[^\d]/g, '');
    if (!sosaText) continue;
    const sosa = parseInt(sosaText, 10);
    if (persons.has(sosa)) continue; // doublons (implexes) : on garde le premier

    const personCell = cells[col.person];
    const personText = cleanText(personCell?.textContent);
    if (!personText || personText === '? ?') continue;
    const link = personCell?.querySelector('a[href]');
    const href = link?.getAttribute('href') || '';
    const { given, surname, title } = splitName(personText, href, pageUrl);

    const get = (field) => (col[field] === undefined ? '' : cleanText(cells[col[field]]?.textContent));
    let url = '';
    if (href) {
      try {
        url = new URL(href, pageUrl || 'https://gw.geneanet.org/').toString();
      } catch {
        url = href;
      }
    }
    persons.set(sosa, {
      sosa,
      given,
      surname,
      title,
      sex: sosa === 1 ? 'U' : sosa % 2 === 0 ? 'M' : 'F',
      birthDate: get('birthDate'),
      birthPlace: get('birthPlace'),
      marriageDate: get('marriageDate'),
      marriagePlace: get('marriagePlace'),
      deathDate: get('deathDate'),
      deathPlace: get('deathPlace'),
      occupation: get('occupation'),
      url,
    });
  }
  if (!persons.size) throw new Error("Aucune personne trouvée dans le tableau d'ascendance.");
  return [...persons.values()].sort((a, b) => a.sosa - b.sosa);
}

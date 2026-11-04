// Génération des fichiers GEDCOM / CSV / JSON à partir des personnes lues.
// Équivalent JavaScript de la classe Person et de run_export_tree() de run.py.
//
// Numérotation Sosa : le père de n est 2n, la mère 2n+1.
// La famille @F{n}@ est celle des parents de n : HUSB @I{2n}@, WIFE @I{2n+1}@, CHIL @I{n}@.

const MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
const FR_MONTHS = {
  janvier: 1, fevrier: 2, mars: 3, avril: 4, mai: 5, juin: 6, juillet: 7,
  aout: 8, septembre: 9, octobre: 10, novembre: 11, decembre: 12,
};

const stripAccents = (s) => s.normalize('NFD').replace(/[̀-ͯ]/g, '');

function exactDate(s) {
  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{1,4})$/); // 17/01/1798
  if (m && +m[2] >= 1 && +m[2] <= 12) return `${+m[1]} ${MONTHS[m[2] - 1]} ${m[3]}`;
  m = s.match(/^(\d{1,2})\/(\d{1,4})$/); // 01/1798
  if (m && +m[1] >= 1 && +m[1] <= 12) return `${MONTHS[m[1] - 1]} ${m[2]}`;
  m = s.match(/^(\d{1,4})$/); // 1798
  if (m) return m[1];
  m = stripAccents(s.toLowerCase()).match(/^(?:le\s+)?(?:(\d{1,2})(?:er)?\s+)?([a-z]+)\s+(\d{1,4})$/); // 17 janvier 1798
  if (m && FR_MONTHS[m[2]]) return `${m[1] ? +m[1] + ' ' : ''}${MONTHS[FR_MONTHS[m[2]] - 1]} ${m[3]}`;
  return null;
}

/** Convertit une date Geneanet ("17/01/1798", "vers 1800", "entre 1800 et 1805"…) au format GEDCOM. */
export function toGedcomDate(raw) {
  const s = String(raw || '').replace(/\s+/g, ' ').trim();
  if (!s) return '';
  const lower = stripAccents(s.toLowerCase());

  let m = lower.match(/^entre (.+) et (.+)$/);
  if (m) {
    const a = exactDate(m[1].trim());
    const b = exactDate(m[2].trim());
    if (a && b) return `BET ${a} AND ${b}`;
  }
  const prefixes = [
    [/^(vers|environ|env\.?|~|ca\.?)\s*/, 'ABT'],
    [/^(avant|av\.?|<)\s*/, 'BEF'],
    [/^(apres|ap\.?|>)\s*/, 'AFT'],
    [/^(peut-etre|probablement)\s*/, 'EST'],
  ];
  for (const [re, keyword] of prefixes) {
    const p = lower.match(re);
    if (p) {
      const d = exactDate(s.slice(p[0].length).trim());
      if (d) return `${keyword} ${d}`;
    }
  }
  const plain = exactDate(s.replace(/^en\s+/i, ''));
  if (plain) return plain;
  return `(${s})`; // date "phrase" GEDCOM : texte libre conservé tel quel
}

// GEDCOM : une valeur ne doit pas contenir de retour à la ligne, et @ s'écrit @@.
const val = (s) => String(s ?? '').replace(/[\r\n]+/g, ' ').replace(/@/g, '@@').trim();

function eventLines(tag, date, place, convertDates) {
  if (!date && !place) return [];
  const lines = [`1 ${tag}`];
  if (date) lines.push(`2 DATE ${val(convertDates ? toGedcomDate(date) : date)}`);
  if (place) lines.push(`2 PLAC ${val(place)}`);
  return lines;
}

/**
 * Construit le fichier GEDCOM 5.5.1.
 * @param {Array} persons  résultat de parseAncestorPage()
 * @param {{treeUrl?: string, convertDates?: boolean}} options
 */
export function buildGedcom(persons, { treeUrl = '', convertDates = true } = {}) {
  const bySosa = new Map(persons.map((p) => [p.sosa, p]));
  const today = new Date();
  const lines = [
    '0 HEAD',
    '1 SOUR scrap_gen',
    '2 NAME scrap_gen (extension Chrome)',
    '2 VERS 1.0',
    '2 CORP scraperfou',
    `1 DATE ${today.getDate()} ${MONTHS[today.getMonth()]} ${today.getFullYear()}`,
    '1 GEDC',
    '2 VERS 5.5.1',
    '2 FORM LINEAGE-LINKED',
    '1 CHAR UTF-8',
    '1 SUBM @SUBM1@',
    '0 @SUBM1@ SUBM',
    '1 NAME scrap_gen',
  ];

  for (const p of persons) {
    const s = p.sosa;
    lines.push(`0 @I${s}@ INDI`);
    lines.push(`1 NAME ${val(p.given)} /${val(p.surname)}/`);
    if (p.given) lines.push(`2 GIVN ${val(p.given)}`);
    if (p.surname) lines.push(`2 SURN ${val(p.surname)}`);
    if (p.title) lines.push(`2 NPFX ${val(p.title)}`);
    lines.push(`1 SEX ${p.sex}`);
    lines.push(...eventLines('BIRT', p.birthDate, p.birthPlace, convertDates));
    if (p.occupation) lines.push(`1 OCCU ${val(p.occupation)}`);
    lines.push(...eventLines('DEAT', p.deathDate, p.deathPlace, convertDates));
    if (treeUrl) lines.push(`1 NOTE Present dans l arbre : ${val(treeUrl)}`);
    if (p.url) lines.push(`1 NOTE Position dans l arbre : ${val(p.url)}`);
    lines.push(`1 NOTE Sosa ${s}`);
    if (bySosa.has(2 * s) || bySosa.has(2 * s + 1)) lines.push(`1 FAMC @F${s}@`);
    if (s > 1 && bySosa.has(Math.floor(s / 2))) lines.push(`1 FAMS @F${Math.floor(s / 2)}@`);
  }

  for (const child of persons) {
    const s = child.sosa;
    const father = bySosa.get(2 * s);
    const mother = bySosa.get(2 * s + 1);
    if (!father && !mother) continue;
    lines.push(`0 @F${s}@ FAM`);
    if (father) lines.push(`1 HUSB @I${2 * s}@`);
    if (mother) lines.push(`1 WIFE @I${2 * s + 1}@`);
    lines.push(`1 CHIL @I${s}@`);
    // L'union est renseignée sur la ligne du père (comme dans run.py), sinon sur celle de la mère.
    const source = father?.marriageDate || father?.marriagePlace ? father : mother;
    if (source) lines.push(...eventLines('MARR', source.marriageDate, source.marriagePlace, convertDates));
  }

  lines.push('0 TRLR');
  return lines.join('\r\n') + '\r\n';
}

const CSV_COLUMNS = [
  ['sosa', 'Sosa'],
  ['given', 'Prénoms'],
  ['surname', 'Nom'],
  ['title', 'Titre'],
  ['sex', 'Sexe'],
  ['birthDate', 'Date de naissance'],
  ['birthPlace', 'Lieu de naissance'],
  ['marriageDate', "Date de l'union"],
  ['marriagePlace', "Lieu de l'union"],
  ['deathDate', 'Date de décès'],
  ['deathPlace', 'Lieu de décès'],
  ['occupation', 'Professions'],
  ['url', 'URL'],
];

/** CSV séparé par ";" avec BOM, lisible directement dans Excel en français. */
export function buildCsv(persons) {
  const esc = (v) => {
    const s = String(v ?? '');
    return /[";\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const rows = [CSV_COLUMNS.map(([, label]) => esc(label)).join(';')];
  for (const p of persons) rows.push(CSV_COLUMNS.map(([key]) => esc(p[key])).join(';'));
  return '﻿' + rows.join('\r\n') + '\r\n';
}

export function buildJson(persons, { treeUrl = '' } = {}) {
  return JSON.stringify({ treeUrl, exportedAt: new Date().toISOString(), persons }, null, 2);
}

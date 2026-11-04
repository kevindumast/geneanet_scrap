import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseGeneanetUrl, buildExportUrl, treeName } from '../lib/geneanet.js';
import { toGedcomDate, buildGedcom, buildCsv } from '../lib/exporters.js';

const URL_ARBRE =
  'https://gw.geneanet.org/124559?lang=fr&iz=2&m=A&p=anne&n=jolivel&oc=3&sosab=10&t=Z&v=6';

test('lecture de l’URL de l’arbre', () => {
  const info = parseGeneanetUrl(URL_ARBRE);
  assert.equal(info.base, '124559');
  assert.equal(info.p, 'anne');
  assert.equal(info.n, 'jolivel');
  assert.equal(info.oc, '3');
  assert.equal(treeName(info), 'jolivel_anne');
  assert.throws(() => parseGeneanetUrl('https://example.com/?p=a&n=b'));
  assert.throws(() => parseGeneanetUrl('https://gw.geneanet.org/124559?lang=fr'));
});

test('construction de l’URL d’export', () => {
  const u = new URL(buildExportUrl(URL_ARBRE, { generations: 20 }));
  assert.equal(u.pathname, '/124559');
  const q = u.searchParams;
  assert.equal(q.get('m'), 'A');
  assert.equal(q.get('t'), 'Z');
  assert.equal(q.get('lang'), 'fr');
  assert.equal(q.get('p'), 'anne');
  assert.equal(q.get('n'), 'jolivel');
  assert.equal(q.get('oc'), '3');
  assert.equal(q.get('v'), '20');
  assert.equal(q.get('occu'), 'on');
});

test('conversion des dates', () => {
  assert.equal(toGedcomDate('17/01/1798'), '17 JAN 1798');
  assert.equal(toGedcomDate('03/1798'), 'MAR 1798');
  assert.equal(toGedcomDate('1798'), '1798');
  assert.equal(toGedcomDate('vers 1800'), 'ABT 1800');
  assert.equal(toGedcomDate('avant 05/08/1994'), 'BEF 5 AUG 1994');
  assert.equal(toGedcomDate('après 1900'), 'AFT 1900');
  assert.equal(toGedcomDate('entre 1950 et 1955'), 'BET 1950 AND 1955');
  assert.equal(toGedcomDate('1er février 1800'), '1 FEB 1800');
  assert.equal(toGedcomDate('quelque part'), '(quelque part)');
  assert.equal(toGedcomDate(''), '');
});

const persons = [
  { sosa: 1, given: 'Arthur', surname: 'DUMAST', title: '', sex: 'U', birthDate: '24/01/1915', birthPlace: 'Mouy', marriageDate: '', marriagePlace: '', deathDate: '', deathPlace: '', occupation: '', url: 'https://gw.geneanet.org/x?p=arthur&n=dumast' },
  { sosa: 2, given: 'Victor', surname: 'DUMAST', title: '', sex: 'M', birthDate: '', birthPlace: '', marriageDate: '28/09/1914', marriagePlace: 'Mouy', deathDate: '', deathPlace: '', occupation: 'Cultivateur', url: '' },
  { sosa: 3, given: 'Gabrielle', surname: 'LE COQ', title: '', sex: 'F', birthDate: '', birthPlace: '', marriageDate: '', marriagePlace: '', deathDate: '', deathPlace: '', occupation: 'Marin @ Calais', url: '' },
];

test('GEDCOM : individus, familles et liens Sosa', () => {
  const ged = buildGedcom(persons, { treeUrl: URL_ARBRE });
  const lines = ged.trim().split('\r\n');
  assert.equal(lines[0], '0 HEAD');
  assert.equal(lines.at(-1), '0 TRLR');
  assert.ok(lines.includes('1 NAME Gabrielle /LE COQ/'));
  assert.ok(lines.includes('2 DATE 24 JAN 1915'));
  assert.ok(lines.includes('1 OCCU Marin @@ Calais'));
  // Famille des parents de 1
  const f = lines.indexOf('0 @F1@ FAM');
  assert.ok(f > 0);
  assert.deepEqual(lines.slice(f, f + 7), [
    '0 @F1@ FAM', '1 HUSB @I2@', '1 WIFE @I3@', '1 CHIL @I1@', '1 MARR', '2 DATE 28 SEP 1914', '2 PLAC Mouy',
  ]);
  // Liens : 1 est enfant de F1, 2 et 3 en sont les conjoints
  const indi = (s) => {
    const start = lines.indexOf(`0 @I${s}@ INDI`);
    const end = lines.findIndex((l, i) => i > start && l.startsWith('0 '));
    return lines.slice(start, end);
  };
  assert.ok(indi(1).includes('1 FAMC @F1@'));
  assert.ok(!indi(1).some((l) => l.startsWith('1 FAMS')));
  assert.ok(indi(2).includes('1 FAMS @F1@'));
  assert.ok(indi(3).includes('1 FAMS @F1@'));
  assert.ok(!indi(2).some((l) => l.startsWith('1 FAMC')));
  // Pas de famille vide
  assert.ok(!lines.includes('0 @F2@ FAM'));
  // Dates brutes si conversion désactivée
  assert.ok(buildGedcom(persons, { convertDates: false }).includes('2 DATE 24/01/1915'));
});

test('CSV', () => {
  const csv = buildCsv(persons);
  assert.ok(csv.startsWith('﻿Sosa;Prénoms;Nom'));
  assert.equal(csv.trim().split('\r\n').length, 4);
});

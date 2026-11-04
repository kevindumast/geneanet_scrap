# Extension Chrome « Geneanet Scrap »

Version extension Chrome du script `run.py` : elle récupère l'ascendance d'une personne
sur Geneanet et l'exporte en **GEDCOM** (ainsi qu'en CSV et JSON), sans Python ni installation.

## Installation (mode développeur)

1. Ouvre `chrome://extensions` dans Chrome (ou Edge / Brave : `edge://extensions`, `brave://extensions`).
2. Active le **Mode développeur** (en haut à droite).
3. Clique sur **Charger l'extension non empaquetée** et choisis le dossier `chrome-extension/` du dépôt.
4. Épingle l'extension « Geneanet Scrap » dans la barre d'outils.

## Utilisation

1. Ouvre sur `gw.geneanet.org` la fiche de la personne de départ (elle sera le Sosa 1).
2. Clique sur l'icône de l'extension : l'URL de l'onglet est déjà remplie.
3. Choisis le nombre de générations (10 par défaut, 100 au maximum).
4. Clique sur **Récupérer l'arbre** : un aperçu des personnes trouvées s'affiche.
5. Télécharge le fichier au format voulu : **GEDCOM**, **CSV** (séparateur `;`, pour Excel) ou **JSON**.
   Le nom proposé reprend celui du script : `arbres/AAAA-MM-JJ__nom_prenom.ged`.

Options :

- **Utiliser l'URL telle quelle** : équivalent de `forceUrlExact=True` dans `run.py`, pour une URL
  qui est déjà une page d'ascendance en tableau (`m=A&t=Z…`).
- **Convertir les dates au format GEDCOM** : `17/01/1798` → `17 JAN 1798`, `vers 1800` → `ABT 1800`,
  `entre 1800 et 1805` → `BET 1800 AND 1805`… Les dates non reconnues sont gardées en texte libre.

La requête vers Geneanet est faite depuis l'onglet Geneanet ouvert, avec ta session :
les arbres visibles quand tu es connecté sont donc exportables.

## Correspondance avec `run.py`

| `run.py`                                         | Extension                                |
| ------------------------------------------------ | ---------------------------------------- |
| `Url_Data` (paramètres et URL d'export)          | `lib/geneanet.js` → `buildExportUrl()`   |
| `get_dataframe_arbre_by_beautifoul_soup_url()`   | `lib/geneanet.js` → `parseAncestorPage()`|
| `Person.get_fiche()` / `run_export_tree()`       | `lib/exporters.js` → `buildGedcom()`     |
| `Person.json`                                    | `lib/exporters.js` → `buildJson()`       |

Numérotation : l'individu `@I{n}@` est le Sosa `n` ; la famille `@F{n}@` réunit ses parents
(`HUSB @I{2n}@`, `WIFE @I{2n+1}@`, `CHIL @I{n}@`). Les lignes « ? ? » et les doublons (implexes)
sont ignorés, comme dans le script.

## Tests

```bash
cd chrome-extension && npm test
```

Le test du parseur HTML tourne dans Chromium via Playwright ; il est ignoré si Playwright n'est pas installé.

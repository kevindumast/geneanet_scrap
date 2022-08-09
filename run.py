import pandas as pd
from urllib.request import Request, urlopen
import re
import numpy as np
import requests
import os
import bs4 as bs

pd.set_option('expand_frame.repr', False)


class Url_Data:
    def __init__(self, url):
        self.url = url
        self.parameters = {'sosab': 10
            , 'ocz': 0
                           # , 'oc': 1
            , 'color': 'r'
            , 't': 'Z'
            , 'birth': 'on'
            , 'birth_place': 'on'
            , 'marr': 'on'
            , 'marr_date': 'on'
            , 'marr_place': 'on'
            , 'death': 'on'
            , 'death_place': 'on'
            , 'occu': 'on'
            , 'gen': 'on'
            , 'repeat': 'on'
            , 'v': 100

                           }
        self.urlExport = self.formater_url(self.url)
        self.name_tree = f"{self.params['n']}_{self.params['p']}".replace('+', ' ')

    def get_param_url(self, url) -> dict:
        query = requests.utils.urlparse(url).query
        return dict(x.split('=') for x in query.split('&'))

    def get_name_tree(self, url) -> str:
        return re.search("([a-z]{3})\/(.*?)\?", url)[2]

    def formater_url(self, url):
        # tableauUrl = "&sosab=10&color=&t=Z&birth=on&birth_place=on&marr=on&marr_date=on&marr_place=on&death=on&death_place=on&gen=on&repeat=on&v=20"
        self.params = self.get_param_url(url)
        self.nameTree = self.get_name_tree(url)
        tableauUrl = "&".join([f'{key}={value}' for key, value in self.parameters.items()])
        return f"https://gw.geneanet.org/{self.nameTree}?lang=fr&m=A&p={self.params['p']}&n={self.params['n']}&{tableauUrl}"


def get_dataframe_arbre_by_url(url: str):
    urlExport = Url_Data(url).urlExport
    df = pd.read_html(urlExport)
    df = df[0]
    df['generation'] = df['Sosa'].apply(lambda x: 1 if 'Génération' in x else 0)
    df = df[df['generation'] == 0]
    try:
        df['Sosa'] = df['Sosa'].apply(lambda x: x.replace(u'\xa0', u'')).astype(int)
    except Exception as e:
        print(e)
    return df


def get_dataframe_arbre_by_beautifoul_soup_url(url: str):
    urlExport = Url_Data(url).urlExport
    req = Request(urlExport)
    html_page = urlopen(req).read()
    soup = bs.BeautifulSoup(html_page, 'lxml')
    parsed_table = soup.find_all('table')
    df = pd.read_html(str(parsed_table), encoding='utf-8')
    df = df[0]
    df.drop_duplicates('Sosa', keep='first', inplace=True)

    df['generation'] = df['Sosa'].apply(lambda x: 1 if 'Génération' in x else 0)
    df = df[df['generation'] == 0]
    try:
        df['Sosa'] = df['Sosa'].apply(lambda x: x.replace(u'\xa0', u'')).astype(int)
    except Exception as e:
        print(e)
    df['url_person'] = get_url_personne(soup, 1)
    # df['url_conjoint'] = get_url_personne(4)
    return df


def get_url_personne(soup, numero_col: int) -> list:
    resultat = []
    for row in soup.find('table').tbody.findAll('tr'):
        if "Génération " not in row.text:
            try:
                # print(row.findAll('td')[1])
                resultat.append([tag.get('href') for tag in row.findAll('td')[numero_col].find_all('a')][0])
            except Exception:
                pass
    return resultat


def get_lastname_with_sosa(chaine_name_surname: str = '', sosa: int = None) -> str:
    if sosa:
        chaine_name_surname = df[df['Sosa'] == sosa]['Personne'].values[0]
    result = re.search('[A-Z\u00C0-\u00DC]{0,10}$', chaine_name_surname.split(',')[0])
    return result[0]


def get_name_with_sosa(chaine_name_surname: str = '', sosa: int = None) -> str:
    if sosa:
        chaine_name_surname = df[df['Sosa'] == sosa]['Personne'].values[0]
    result = re.sub('[A-Z\u00C0-\u00DC]{0,100}$', '', chaine_name_surname.split(',')[0])
    return result.strip()


def get_titre_with_sosa(chaine_name_surname: str = '', sosa: int = None) -> str:
    if sosa:
        chaine_name_surname = df[df['Sosa'] == sosa]['Personne'].values[0]
    try:
        return chaine_name_surname.split(',')[1].strip()
    except Exception as e:
        return False


def get_url_person_in_tree(sosa: int) -> str:
    try:
        href = df[df['Sosa'] == sosa]['url_person'].values[0]
        return f"https://gw.geneanet.org/{href}"
    except Exception as e:
        return False


def get_job_with_sosa(sosa: int = None) -> str:
    try:
        job = df[df['Sosa'] == sosa].to_dict('records')[0]['Professions']
        return False if pd.isnull(job) else job
    except Exception as e:
        return False


def get_birth_information_with_sosa(sosa: int = None, typeInfos: str = '') -> str:
    try:
        transco = {'date': 'Date de naissance', 'address': 'Lieu de naissance'}
        result = df[df['Sosa'] == sosa][list(transco.values())].fillna('').to_dict('records')[0]
        return result[transco[typeInfos]]
    except Exception as e:
        return False


def get_death_information_with_sosa(sosa: int = None, typeInfos: str = '') -> str:
    try:
        transco = {'date': 'Date de décès', 'address': 'Lieu de décès'}
        result = df[df['Sosa'] == sosa][list(transco.values())].fillna('').to_dict('records')[0]
        return result[transco[typeInfos]]
    except Exception as e:
        return False


def get_union_information_with_sosa(sosa: int = None, typeInfos: str = '') -> str:
    try:
        transco = {'date': "Date de l'union", 'address': "Lieu de l'union"}
        result = df[df['Sosa'] == sosa][list(transco.values())].fillna('').to_dict('records')[0]
        return result[transco[typeInfos]]
    except Exception as e:
        return False


def get_number_of_family_with_sosa(sosa: int, memberType: str = ''):
    return (sosa) // 2 if memberType == 'Father' else (sosa - 1) // 2


def get_parent(sosa: int, memberType: str):
    # sosa = 5
    if memberType == 'Father':
        return df[df['Sosa'] == (sosa * 2)].to_dict('records')[0]
    elif memberType == 'Mother':
        return df[df['Sosa'] == (sosa * 2) + 1].to_dict('records')[0]


def get_children_sosa(sosa: int):
    result = 'P' if (sosa % 2) == 0 else 'I'
    sosa_children = sosa / 2 if result == "P" else (sosa - 1) / 2
    return df[df['Sosa'] == sosa_children].to_dict('records')[0]['Sosa']


def get_conjoint(sosa: int, memberType: str):
    # sosa = 5
    if memberType == 'Husb':
        return df[df['Sosa'] == (sosa - 1)].to_dict('records')[0]
    elif memberType == 'Wife':
        return df[df['Sosa'] == (sosa + 1)].to_dict('records')[0]


def get_famc(sosa: int):
    # sosa = 5
    if sosa == 1:
        return 1
    else:
        return ((sosa * 2) // 2) if get_sexe_by_sosa(sosa=3) == 'M' else (((sosa * 2) + 1) // 2)


def get_sexe_by_sosa(sosa: int):
    if sosa == 1:
        return 'U'
    else:
        return 'M' if (sosa % 2) == 0 else 'F'


def test_person_exist(sosa: int) -> bool:
    return not df[df['Sosa'] == sosa].empty


class Person:
    def __init__(self, sosa, url=None):
        self.sosa = sosa
        self.fiche = []
        self.sexe = get_sexe_by_sosa(sosa=self.sosa)
        self.json = {}
        self.url = url

    def get_fiche(self):

        if test_person_exist(self.sosa):
            # self.add_header()
            self.add_information_person()
            self.add_birth_info()
            self.add_job()
            self.add_death_info()
            self.add_union_infos()
            self.add_url_position_in_tree()
            return self.fiche
        else:
            return "la personne n existe pas"

    def add_url_position_in_tree(self) -> None:
        self.urlPerson = get_url_person_in_tree(sosa=self.sosa)

        # self.fiche.append(f"0 @I{self.sosa}@  NOTE")
        # self.fiche.append(f"1 CONT Present dans l arbre : {self.url}")
        # self.fiche.append(f"1 CONT Position dans l arbre : {self.urlPerson}")

        self.fiche.append(f"1 NOTE Present dans l arbre : {self.url}")
        self.fiche.append(f"1 NOTE Position dans l arbre : {self.urlPerson}")
        self.json.update({'urlTree': self.url, 'urlPerson': self.urlPerson})
    def add_information_person(self) -> None:
        self.fiche.append(f"0 @I{self.sosa}@ INDI")
        self.lastname = get_lastname_with_sosa(sosa=self.sosa)
        self.name = get_name_with_sosa(sosa=self.sosa)
        self.fiche.append(f"1 NAME {self.name} / {self.lastname} /")
        self.titre = get_titre_with_sosa(sosa=self.sosa)
        self.fiche.append(f"2 GIVN {self.name}")
        self.fiche.append(f"2 SURN {self.lastname}")

        if self.titre:
            self.fiche.append(f"2 NPFX {self.titre}")
            self.json.update({'titre': self.titre})

        self.fiche.append(f"1 SEX {self.sexe}")


        self.json = {'name': self.name
            , 'lastname': self.lastname
            , 'sexe': self.sexe
                     }


    def add_birth_info(self) -> None:
        self.fiche.append("1 BIRT")
        self.dateBirth = get_birth_information_with_sosa(sosa=self.sosa, typeInfos='date')
        if self.dateBirth:
            self.fiche.append(f"2 DATE {self.dateBirth}")

        self.addressBirth = get_birth_information_with_sosa(sosa=self.sosa, typeInfos='address')
        if self.addressBirth:
            self.fiche.append(f"2 PLAC {self.addressBirth}")

    def add_job(self) -> None:
        self.job = get_job_with_sosa(sosa=self.sosa)
        if self.job != False:
            self.fiche.append(f"1 OCCU {self.job}")
            self.json.update({'job': self.job})

    def add_death_info(self) -> None:
        deathNote = ["1 DEAT"]
        self.dateDeath = get_death_information_with_sosa(sosa=self.sosa, typeInfos='date')
        if self.dateDeath:
            deathNote.append(f"2 DATE {self.dateDeath}")

        self.addressDeath = get_death_information_with_sosa(sosa=self.sosa, typeInfos='address')
        if self.addressDeath:
            deathNote.append(f"2 PLAC {self.addressDeath}")

        if len(deathNote) == 1:
            return None
        else:
            self.fiche.extend(deathNote)

    def add_union_infos(self) -> None:
        if self.sosa != 1:
            self.numberFam = self.sosa // 2
            unionNote = [f"1 FAMC @F{get_famc(sosa=self.sosa)}@"
                , f"1 FAMS @F{self.numberFam}@"]

            if get_sexe_by_sosa(self.sosa) == 'F':
                unionNote.append("2 TYPE married")
            else:
                unionNote.append(f"0 @F{self.numberFam}@ FAM")
                try:
                    unionNote.append(
                        f"1 HUSB @I{self.sosa if self.sexe == 'M' else get_conjoint(sosa=self.sosa, memberType='Husb')['Sosa']}@")
                except Exception as e:
                    pass
                try:
                    unionNote.append(
                        f"1 WIFE @I{self.sosa if self.sexe == 'F' else get_conjoint(sosa=self.sosa, memberType='Wife')['Sosa']}@")
                except Exception as e:
                    pass

                self.child = get_children_sosa(sosa=self.sosa)
                if self.child:
                    unionNote.append(f"1 CHIL @I{self.child}@")

                self.dateUnion = get_union_information_with_sosa(sosa=self.sosa, typeInfos='date')

                if self.dateUnion:
                    unionNote.append("1 MARR")
                    unionNote.append(f"2 DATE {self.dateUnion}")

                self.addressUnion = get_union_information_with_sosa(sosa=self.sosa, typeInfos='address')
                if self.addressUnion:
                    unionNote.append(f"2 PLAC {self.addressUnion}")

            if len(unionNote) == 1:
                return None
            else:
                self.fiche.extend(unionNote)
        else:
            unionNote = ["1 FAMC @F2@"]
            self.fiche.extend(unionNote)


def delete_file_if_exist(nomGed: str):
    if os.path.exists(f'arbres/{nomGed}.ged'):
        os.remove(f'arbres/{nomGed}.ged')
    else:
        print("Can not delete the file as it doesn't exists")


def run_export_tree():
    delete_file_if_exist(nomGed=nomGed)
    with open(f"arbres/{str(pd.to_datetime('today'))[:10]}__{nomGed}.ged", 'a', encoding='utf-8') as f:
        f.write('\n' + "0 HEAD")
        f.write('\n' + "1 SOUR scrap_gen")
        f.write('\n' + "2 NAME scrap_gen")
        f.write('\n' + "2 VERS 1.0")
        f.write('\n' + "2 CORP scraperfou")
        f.write('\n' + "1 CHAR UTF-8")

        for person in df['Sosa'].unique():
            print(person)
            for row in Person(sosa=person, url=url).get_fiche():
                # print(row)
                f.write('\n' + row)
        print(f"export file {nomGed} : end -> {len(df['Sosa'].unique())} personnes")


url = "https://gw.geneanet.org/vayssej?n=cailhol&oc=&p=baptiste"
nomGed = Url_Data(url).name_tree
Url_Data(url).urlExport
df = get_dataframe_arbre_by_url(url=url)
df = get_dataframe_arbre_by_beautifoul_soup_url(url=url)
df = df[df['Personne'] != "? ?"]
df.head()


df[df['Personne'] == "Raymond RESSEGUIER"]
df[df['Sosa'] == 30060]

p = Person(sosa=30060, url=url)
p.get_fiche()
p.json

run_export_tree()




df = df[df['Personne'] != "? ?"]

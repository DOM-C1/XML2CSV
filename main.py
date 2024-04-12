
"""This file extracts the most recent file from an s3 bucket and uploads it
to another s3 bucket"""

from datetime import datetime
import re
import xml.etree.ElementTree as ET

import pandas as pd
import spacy
from rapidfuzz import process, fuzz
from boto3 import client
from dotenv import load_dotenv
from os import environ as ENV

COMB_POSTAL_REGEX = re.compile(r"(\d{5}(?:[-\s]\d{4})?)|([A-Za-z][A-Ha-hJ-Yj-y]?[0-9][A-Za-z0-9]? ?[0-9][A-Za-z]{2}|[Gg][Ii][Rr] ?0[Aa]{2})|([A-Z][0-9][A-Z][-\s]*[0-9][A-Z][0-9])")
EMAIL_REGEX = re.compile(r'^\S*@\S*$')
 


def parse_xml_data(root):
    """Creates the dataframe with the base information needed"""
    data = []
    affil_data = []
    for pubmed_article in root.findall('.//PubmedArticle'):
        for author in pubmed_article.findall('.//AuthorList/Author'):
            if author:
                try:

                    last_name = author.find('LastName').text
                    first_name = author.find('ForeName').text
                    initials = author.find('Initials').text
                    affiliation_info = author.find('AffiliationInfo')

                    if affiliation_info:
                        affiliations = affiliation_info.find(
                            'Affiliation').text
                        email = get_email(affiliations)
                        postal = get_postal_code(affiliations)
                        

                    name = f"{last_name}, {first_name} {initials}"

                    title = pubmed_article.find(".//Title").text
                    year = pubmed_article.find(".//Year").text
                    pmid = pubmed_article.find(".//PMID").text
                    keyword = [keyword.text for keyword in pubmed_article.find(
                        './/KeywordList')]
                    ui = [item.get("UI") for item in pubmed_article.findall(
                        ".//*[@UI]") if item.get("UI").startswith('D0')]

                    data.append({
                        'Name': name,
                        'Title': title,
                        'Year': year,
                        'PMID': pmid,
                        'Keywords': keyword,
                        'UI': ui,
                        'email': email,
                        'Postal Code': postal


                    })
                    affil_data.append({'Affiliation': affiliations})
                    
                    
                except:
                    pass

    return data, affil_data


def get_email(affil: list) -> str:
    """Given a string, return the part that is an email address."""
    match = EMAIL_REGEX.search(affil)
    return match.group() if match else 'None'

def get_postal_code(affil: str) -> str:
    """Given a string return the postal code."""
    postal = COMB_POSTAL_REGEX.search(affil)
    return postal.group() if postal else 'None' 


def get_country(col: spacy.tokens.doc.Doc) -> str:
    """Given a column find all of the countries"""
    countries = [token for token in col.ents if token.label_ == 'GPE']
    return countries[-1] if countries else 'None'


def get_inst_df():
    """Returns all of the insitutions"""
    return pd.read_csv('./institutes.csv')


def get_institutions_from_affil(affil: str, institutions: pd.DataFrame) -> str:
    """Given the affiliation string, extract the institution with the highest similarity score using RapidFuzz."""
    if affil in match_cache:
        return match_cache[affil]
    institution_names = institutions['name'].tolist()
    best_match = process.extractOne(affil, institution_names, scorer=fuzz.partial_ratio)
    match_cache[affil] = best_match[0]

    return best_match[0]



def get_grid_id_from_name(name: str, institutions: pd.DataFrame) -> str:
    """Given the official GRID name extract the associated 
       GRID ID."""
    matches = institutions[institutions['name'] == name]['grid_id']
    return matches.iloc[0] if not matches.empty else 'No good match'



def get_recent_obj_key(s3: boto3.client) -> str:
    """Finds the most recent object key that was posted in the 'sigma-pharmazer-input' bucket under
       under the folder, 'dom'."""
    bucket_name = 'sigma-pharmazer-input'
    response = s3.list_objects_v2(Bucket=bucket_name)
    files = [(obj['Key'], obj['LastModified']) for obj in response['Contents'] if obj['Key'].startswith('dom') and obj['Key'].endswith('xml')]
    if files:
        return max(files, key=lambda x: x[1])[0]


def get_xml(object_key: str) -> None:
    """Given the object key, download the corresponding file."""
    bucket_name = 'sigma-pharmazer-input'
    local_filename = 'data.xml'
    s3.download_file(bucket_name, object_key, local_filename)


if __name__ == '__main__':
    load_dotenv()
    s3 = client("s3",
                aws_access_key_id=ENV["AWS_KEY"],
                aws_secret_access_key=ENV["AWS_SECRET_KEY"])

    obj_key = get_recent_obj_key(s3)
    get_xml(obj_key)


    tree = ET.parse('./data.xml')
    root = tree.getroot()
    data = parse_xml_data(root)


    data_dict = data[0]
    affil = data[1]
    data = pd.DataFrame(data_dict)
    affil_df = pd.DataFrame(affil)


    nlp = spacy.load("en_core_web_sm")
    data['Country'] = affil_df['Affiliation'].apply(
        lambda x: get_country(nlp(x)))
    

    institution = get_inst_df()
    match_cache = {}
    data['GRID Institution'] = affil_df['Affiliation'].apply(
        lambda x: get_institutions_from_affil(x, institution))
    data['GRID ID'] = data['GRID Institution'].apply(
        lambda x: get_grid_id_from_name(x, institution))
    

    dt = datetime.now()
    dt = dt.strftime("%d-%m-%Y %H:%M")
    data.to_csv(f'data {dt}.csv')
    s3.upload_file(f'data {dt}.csv',
                   'sigma-pharmazer-output', f'dom/data {dt}.csv')

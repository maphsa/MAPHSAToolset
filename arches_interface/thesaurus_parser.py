from rdflib import Graph
import json


def extract_thesaurus(thesaurus_url: str) -> dict:

    g = Graph()
    g.parse(thesaurus_url)
    # g.bind("skos", SKOS)
    target_subjects = []
    concept_data = {
        'concepts': {}
    }

    # Query to see all the triples
    '''
    query = """
        SELECT DISTINCT ?a ?b ?c
        WHERE {
            ?a ?b ?c .
        }"""

    qres = g.query(query)
    for row in qres:
        print(f"{row.a}, {row.b}, {row.c}")
    '''

    # Query to fetch the title of the thesaurus
    query = """
    SELECT DISTINCT ?a ?title
    WHERE {
        ?a dcterms:title ?title .
    }"""

    qres = g.query(query)
    concept_data['thesaurus_title'] = json.loads(list(qres)[0][1].__str__())['value']

    # Query to fetch all preferred labels (likely in English)
    query = """
        SELECT ?pref_label
        WHERE {
            ?concept_id ?b skos:Concept .
            ?concept_id skos:prefLabel ?pref_label .
        }"""
    qres = g.query(query)

    for row in qres:
        concept_name = f"{json.loads(row.pref_label.__str__())['value']}"
        concept_data['concepts'][concept_name] = None

    query = """
            SELECT ?pref_label ?scope_note
            WHERE {
                ?concept_id ?b skos:Concept .
                ?concept_id skos:prefLabel ?pref_label .
                ?concept_id skos:scopeNote ?scope_note .
            }"""
    qres = g.query(query)

    for row in qres:
        concept_name = f"{json.loads(row.pref_label.__str__())['value']}"
        concept_note = json.loads(row.scope_note.__str__())['value']
        concept_data['concepts'][concept_name] = concept_note

    return concept_data

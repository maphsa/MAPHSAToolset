import argparse
import json
import uuid

from jinja2 import Template

import arches_interface
from database_interface import *
from rdflib import Graph

from exceptions.exceptions import MAPHSARDMIntegrityException
from gdrive_interface.thesaurus_interface import get_online_thesaurus_data

import tqdm

def get_thesaurus_concept_data(thesaurus_id):
    g = Graph()
    rdm_url = f"{DatabaseInterface.get_target_env_base_url()}/concepts/export/{thesaurus_id}"
    g.parse(rdm_url, format='xml')
    return g


def parse_thesaurus_concept_scheme_data(concept_graph, thesaurus_id):
    thesaurus_concept_scheme_data = arches_interface.ArchesConceptScheme()
    thesaurus_concept_scheme_data._id = thesaurus_id

    # Query to fetch all ids and preferred labels (likely in English)
    query = """
            SELECT ?c
            WHERE {
                ?a dcterms:title ?c .
            }"""
    qres = concept_graph.query(query)
    thesaurus_concept_scheme_data._title = json.loads(list(qres)[0][0].value.__str__())['value']

    # Query to fetch all ids and preferred labels (likely in English)
    query = """
        SELECT ?pref_label ?concept_id
        WHERE {
            ?concept_id ?b skos:Concept .
            ?concept_id skos:prefLabel ?pref_label .
        }"""
    qres = concept_graph.query(query)

    for row in qres:
        concept_id = row.concept_id.__str__().replace(f"{DatabaseInterface.get_target_env_base_url()}/", '')
        concept_name = f"{json.loads(row.pref_label.__str__())['value']}"
        target_concept = arches_interface.ArchesConcept(concept_id, concept_name)
        thesaurus_concept_scheme_data.members[target_concept.id] = target_concept

    # Query to fetch scope notes
    query = """
            SELECT ?concept_id ?pref_label ?scope_note
            WHERE {
                ?concept_id ?b skos:Concept .
                ?concept_id skos:scopeNote ?scope_note .
                ?concept_id skos:prefLabel ?pref_label .
            }"""
    qres = concept_graph.query(query)

    for row in qres:
        concept_id = row.concept_id.__str__().replace(f"{DatabaseInterface.get_target_env_base_url()}/", '')
        concept_note = json.loads(row.scope_note.__str__())['value']
        thesaurus_concept_scheme_data.members[concept_id].note = concept_note

    return thesaurus_concept_scheme_data


def get_thesaurus_collection_data():
    g = Graph()
    rdm_url = f"{DatabaseInterface.get_target_env_base_url()}/concepts/export/collections"
    g.parse(rdm_url, format='xml')
    return g


def parse_thesaurus_collection_data(target_concept_data):
    thesaurus_collections = {
    }

    collection_graph = get_thesaurus_collection_data()

    query = """
         SELECT ?subject ?object
         WHERE {
             ?subject skos:prefLabel ?object .
         }"""

    qres = collection_graph.query(query)
    for row in qres:
        collection_name = json.loads((row[1].__str__()))['value']
        if collection_name in arches_interface.ARCHES_SYSTEM_COLLECTIONS:
            continue
        try:
            if collection_name not in list(target_concept_data.concept_schemes.keys()):
                raise MAPHSARDMIntegrityException(f"Collection name {collection_name} not found in concept schemes")
        except MAPHSARDMIntegrityException as mrdmie:
            print(mrdmie)

        collection_id = row[0].replace(f"{DatabaseInterface.get_target_env_base_url()}/", '')

        concept_collection_data = arches_interface.ArchesConceptCollection()
        concept_collection_data._id = collection_id
        concept_collection_data._title = collection_name
        thesaurus_collections[collection_name] = concept_collection_data

    # Check for missing collections that are present as concept schemes
    try:
        schemes_missing_collection = target_concept_data.concept_schemes.keys() - thesaurus_collections.keys()
        if len(schemes_missing_collection) > 0:
            raise MAPHSARDMIntegrityException(f"Concept schemes {schemes_missing_collection} "
                                              f"missing matching concept collection")
    except MAPHSARDMIntegrityException as mrdmie:
        print(mrdmie)

    query = """
             SELECT ?subject ?object
             WHERE {
                 ?subject skos:member ?object .
             }"""

    qres = collection_graph.query(query)
    for row in qres:
        collection_id = row[0].replace(f"{DatabaseInterface.get_target_env_base_url()}/", '')
        concept_member_id = row[1].replace(f"{DatabaseInterface.get_target_env_base_url()}/", '')
        try:
            collection_name = [v.title for v in thesaurus_collections.values() if v.id == collection_id][0]
        except IndexError as ie:  # Skip Arches system collections
            continue

        try:
            concept_member = target_concept_data.concepts[concept_member_id]
            concept_collection_data = thesaurus_collections[collection_name]
            concept_collection_data.members[concept_member_id] = concept_member
        except KeyError:
            print(f"Unable to match id {concept_member_id} with a concept in a scheme")
            continue

    for concept_collection in thesaurus_collections.values():
        target_concept_data.add_concept_collection(concept_collection)
        cc_id = concept_collection.title
        try:
            scheme_collection_concept_mismatch = sccm = (target_concept_data.concept_schemes[cc_id].members.keys()
                                                         - target_concept_data.collections[cc_id].members.keys())

            sccm = [target_concept_data.concepts[c].pref_label for c in sccm]

            if len(scheme_collection_concept_mismatch) > 0:
                raise MAPHSARDMIntegrityException(f"Concepts {sccm} from the concept scheme {cc_id} "
                                                  f"missing in matching collection")
        except MAPHSARDMIntegrityException as mrdmie:
            print(mrdmie)

    return target_concept_data


def get_target_thesauri_concept_collection_data():
    concept_collection_names = DatabaseInterface.run_script(
        # 'getConceptCollectionMembers',
        'getConceptCollectionNames',
        {},
        target_db_connection=DatabaseInterface.get_db_connection(TARGET_ROLE))

    thesauri_cc_data = arches_interface.ArchesConceptCollectionData()

    for ccn in sorted([(ccn[1], ccn[2], ccn[0]) for ccn in concept_collection_names]):

        if ccn[1] == 'ConceptScheme' and ccn[0] not in arches_interface.ARCHES_SYSTEM_COLLECTIONS:
            concept_scheme_data = parse_thesaurus_concept_scheme_data(get_thesaurus_concept_data(ccn[2]), ccn[2])
            thesauri_cc_data.add_concept_scheme(concept_scheme_data)

    thesauri_cc_data = parse_thesaurus_collection_data(thesauri_cc_data)

    return thesauri_cc_data


def verify_rdm_integrity():
    tt_cc_data = get_target_thesauri_concept_collection_data()

    print(f"Loaded {len(tt_cc_data.concept_schemes)}"
          f" thesauri in target Arches deployment with {len(tt_cc_data.concepts)} concepts")

    return


def report_integrity_exception(mmie: MAPHSARDMIntegrityException):
    print(mmie)
    if mmie.missing_elements:
        print(mmie.missing_elements)
    print()


def cross_validate_online_thesaurus_concept_collections():
    online_thesaurus_data = get_online_thesaurus_data()

    target_thesauri_collection_data = get_target_thesauri_concept_collection_data()

    # Check if both online thesaurus and target have the same thesauri
    online_missing_thesauri = set(online_thesaurus_data.keys()).difference(
        set(target_thesauri_collection_data.concept_schemes.keys()))
    if len(online_missing_thesauri) > 0:
        report_integrity_exception(
            MAPHSARDMIntegrityException(f"Online thesauri missing in target Arches deployment",
                                        online_missing_thesauri))

    target_missing_thesauri = set(target_thesauri_collection_data.concept_schemes.keys()).difference(
        set(online_thesaurus_data.keys()))
    if len(target_missing_thesauri) > 0:
        report_integrity_exception(
            MAPHSARDMIntegrityException(f"Target Arches deployment thesauri missing in the online thesaurus",
                                        target_missing_thesauri))

    # Check if both online thesauri and target have the same members
    for tn in [tn for tn in online_thesaurus_data.keys()
               if tn not in online_missing_thesauri.union(target_missing_thesauri)]:

        ol_th_data = set(online_thesaurus_data[tn])
        tg_th_data = set(c.pref_label for c in target_thesauri_collection_data.concept_schemes[tn].members.values())

        online_missing_members = ol_th_data.difference(tg_th_data)
        if len(online_missing_members) > 0:
            report_integrity_exception(MAPHSARDMIntegrityException(
                f"Online thesauri {tn} members missing in target Arches deployment",
                online_missing_members))

        target_missing_members = tg_th_data.difference(ol_th_data)
        if len(target_missing_members) > 0:
            report_integrity_exception(MAPHSARDMIntegrityException(
                f"Target Arches deployment {tn} members missing in online thesauri",
                target_missing_members))


def cross_validate_origin_target_concept_collections():
    origin_concept_collection_data_rows = DatabaseInterface.run_script(
        # 'getConceptCollectionMembers',
        'getOriginConceptMembers',
        {},
        target_db_connection=DatabaseInterface.get_db_connection(ORIGIN_ROLE))

    origin_concept_collection_data = {}
    for row in origin_concept_collection_data_rows:
        if row[0] not in origin_concept_collection_data.keys():
            origin_concept_collection_data[row[0]] = []
        origin_concept_collection_data[row[0]].append(row[1])

    print(f"Loaded {len(origin_concept_collection_data.keys())} thesauri from origin database "
          f"with {sum(len(x) for x in origin_concept_collection_data.values())} total concepts")

    target_thesauri_cc_data = get_target_thesauri_concept_collection_data()

    print(f"Loaded {len(target_thesauri_cc_data.concept_schemes.keys())} thesauri from target Arches deployment "
          f"with {sum([len(x.members.keys()) for x in target_thesauri_cc_data.concept_schemes.values()])} total concepts")

    # Check if both origin and target have the same thesauri
    origin_missing_thesauri = set(origin_concept_collection_data.keys()).difference(
        set(target_thesauri_cc_data.concept_schemes.keys()))
    if len(origin_missing_thesauri) > 0:
        report_integrity_exception(
            MAPHSARDMIntegrityException(f"Origin thesauri missing in target Arches deployment",
                                        origin_missing_thesauri))

    target_missing_thesauri = set(target_thesauri_cc_data.concept_schemes.keys()).difference(
        set(origin_concept_collection_data.keys()))
    if len(target_missing_thesauri) > 0:
        report_integrity_exception(
            MAPHSARDMIntegrityException(f"Target Arches deployment thesauri missing in origin",
                                        target_missing_thesauri))

    # Check if both origin and target have the same thesauri members
    for tn in [tn for tn in origin_concept_collection_data.keys()
               if tn not in origin_missing_thesauri.union(target_missing_thesauri)]:

        org_th_data = set(origin_concept_collection_data[tn])
        tg_th_data = set(c.pref_label for c in target_thesauri_cc_data.concept_schemes[tn].members.values())

        origin_missing_members = org_th_data.difference(tg_th_data)
        if len(origin_missing_members) > 0:
            report_integrity_exception(MAPHSARDMIntegrityException(
                f"Origin thesauri {tn} members missing in target Arches deployment",
                origin_missing_members))

        target_missing_members = tg_th_data.difference(org_th_data)
        if len(target_missing_members) > 0:
            report_integrity_exception(MAPHSARDMIntegrityException(
                f"Target Arches deployment {tn} members missing in origin thesauri",
                target_missing_members))


def export_target_rdm():
    concept_collection_names = DatabaseInterface.run_script(
        # 'getConceptCollectionMembers',
        'getConceptCollectionNames',
        {},
        target_db_connection=DatabaseInterface.get_db_connection(TARGET_ROLE))

    for ccn in sorted([(ccn[1], ccn[2], ccn[0]) for ccn in concept_collection_names]):

        if ccn[1] == 'ConceptScheme' and ccn[0] not in arches_interface.ARCHES_SYSTEM_COLLECTIONS:
            export_thesaurus_rdf(ccn[2])

    export_collection_rdf()


def export_thesaurus_rdf(thesaurus_id):
    concept_scheme_graph = get_thesaurus_concept_data(thesaurus_id)
    query = """
                SELECT ?c
                WHERE {
                    ?a dcterms:title ?c .
                }"""
    qres = concept_scheme_graph.query(query)

    concept_scheme_title = str(json.loads(list(qres)[0][0].value.__str__())['value'])
    concept_scheme_title = concept_scheme_title.replace(' ','_')
    rdf_url = f"{arches_interface.OUTPUT_PATH}/reference_data/{concept_scheme_title}.xml"
    with open(rdf_url, "w") as outfile:
        outfile.write(concept_scheme_graph.serialize(format="xml"))


def export_collection_rdf():
    collection_graph = get_thesaurus_collection_data()

    query = """
             SELECT ?subject ?object
             WHERE {
                 ?subject skos:prefLabel ?object .
             }"""

    rdf_url = f"{arches_interface.OUTPUT_PATH}/reference_data/collections/collections.xml"
    with open(rdf_url, "w") as outfile:
        outfile.write(collection_graph.serialize(format="xml"))

def create_business_data(source_business_data: dict):
    export_data = {'business_data': { 'resources': []}}

    # for bd_id, bd in source_business_data.items():

    # Debug mode only, also disable for loop after
    # bd_id = 50
    # bd = source_business_data[bd_id]

    # TODO THIS IS VERY CONCERNING, REDUNDANT DATA?
    present_maphsa_ids = set()
    dropped_data_items = []
    for index, bd in tqdm.tqdm(source_business_data.items()):

        # TODO THIS IS VERY CONCERNING, REDUNDANT DATA?
        if bd['maphsa_id'] in present_maphsa_ids:
            dropped_data_items.append(bd)
            continue

        bd['graph_id'] = "7ec8e4c2-1cc8-11ee-8c57-996f267db53e"
        bd["publication_id"] = "fb322262-701e-11ef-ba6a-0242ac120007"

        ####################################################################################################################
        bd['heritage_location_summary_node_id'] = "7ec8e4c5-1cc8-11ee-8c57-996f267db53e" # Group node

        bd["heritage_location_general_description_node_id"] = "7ec8e504-1cc8-11ee-8c57-996f267db53e"
        # raw bd["heritage_location_general_description"] value
        # TODO apply proper filtering
        bd["heritage_location_general_description"] = bd["heritage_location_general_description"].strip() if "heritage_location_general_description" in bd.keys() else None

        ####################################################################################################################
        bd["heritage_location_name_node_id"] = "7ec8e505-1cc8-11ee-8c57-996f267db53e" # Group node
        # raw bd["heritage_location_name"] value

        bd["heritage_location_name_type_node_id"] = "2d7eb348-57b6-11ee-9bdf-c71756fbf602"
        bd["heritage_location_name_type_value_id"] = "be514849-797d-4449-97c8-3f7f97742e7e" # Primary Name

        ####################################################################################################################
        bd['maphsa_id_node_id'] = "7ec8e4cb-1cc8-11ee-8c57-996f267db53e" # Group node
        # raw bd["maphsa_id"] value

        ####################################################################################################################
        bd['geometry_node_id'] = "7ec8e4cd-1cc8-11ee-8c57-996f267db53e" # Group node

        bd['grid_square_node_id'] = "efa52832-3ec7-11ef-ad06-5945cbceeba1"

        ####################################################################################################################
        bd['spatial_coordinates_node_id'] = "7ec8e4fa-1cc8-11ee-8c57-996f267db53e" # Group node
        # composed bd["geometry"] value

        bd['location_certainty_node_id'] = "7ec8e4d4-1cc8-11ee-8c57-996f267db53e"
        bd['location_certainty_value_id'] = "2438d28d-dab8-480d-a24a-4a6e7dc388cd" # High

        bd['spatial_coordinates_reference_system_datum_node_id'] = "7ec8e4ea-1cc8-11ee-8c57-996f267db53e"
        bd['spatial_coordinates_reference_system_datum_value_id'] = "5ee9eb5e-873e-4e9b-b6f1-cc06b324ac77" # SIRGAS2000

        bd['geometry_extent_certainty_node_id'] = "7ec8e4f2-1cc8-11ee-8c57-996f267db53e"
        bd['geometry_extent_certainty_value_id'] = "4b12557b-a709-4927-adb6-ffe801da47d7" # High

        bd['tile_ids'] = [str(uuid.uuid4()) for x in range(5)]

        template_string = open(f"{arches_interface.TEMPLATE_PATH}/arches_resource.j2", 'r').read()
        template = Template(template_string)
        rendered_resource = template.render(bd)
        try:
            resource_data = json.loads(rendered_resource)
        except json.decoder.JSONDecodeError as jde:
            print(jde)
            dropped_data_items.append(bd)

        export_data['business_data']['resources'].append(resource_data)
        present_maphsa_ids.add(bd['maphsa_id'])

    print(f"Process finished with {len(export_data['business_data']['resources'])} exported sites and {len(dropped_data_items)} failed exported sites")

    return export_data


def process_subcommand(args: argparse.Namespace):
    subcommand = args.subcommand[1]

    if subcommand == arches_interface.EXPORT_TARGET_RDM_SUBCOMMAND:
        export_target_rdm()

    elif subcommand == arches_interface.VERIFY_TARGET_CC_INTEGRITY_SUBCOMMAND:
        verify_rdm_integrity()

    elif subcommand == arches_interface.CROSS_VALIDATE_ORIGIN_CONCEPT_COLLECTIONS_SUBCOMMAND:
        cross_validate_origin_target_concept_collections()

    elif subcommand == arches_interface.CROSS_VALIDATE_ONLINE_THESAURUS_CONCEPT_COLLECTIONS_SUBCOMMAND:

        cross_validate_online_thesaurus_concept_collections()

    elif subcommand == arches_interface.COMPARE_CONCEPT_COLLECTIONS_SUBCOMMAND:
        targ_col_data = DatabaseInterface.run_script('getConceptCollectionNames', {},
                                                     target_db_connection=
                                                     DatabaseInterface.get_db_connection(TARGET_ROLE))
        ref_col_data = DatabaseInterface.run_script('getConceptCollectionNames', {},
                                                    target_db_connection=
                                                    DatabaseInterface.get_db_connection(REFERENCE_ROLE))

        raise NotImplemented()

    else:
        raise Exception(f"Unknown arches interface command {subcommand}")

    print(f"{subcommand} finished execution")

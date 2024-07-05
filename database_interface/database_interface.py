import argparse

import psycopg2
from jinja2 import Template

import database_interface

from sqlalchemy.ext.automap import automap_base, generate_relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


class DatabaseInterface:
    _db_connection_data_pool = {}
    _db_connection_pool = {}

    _concept_id_mappings = None

    @classmethod
    def initialize(cls, args):
        if args.origin:
            cls._db_connection_data_pool[database_interface.ORIGIN_ROLE] \
                = database_interface.DB_ORIGIN_SETTINGS[args.origin]
        if args.target:
            cls._db_connection_data_pool[database_interface.TARGET_ROLE] \
                = database_interface.DB_TARGET_SETTINGS[args.target]

        if args.reference:
            cls._db_connection_data_pool[database_interface.REFERENCE_ROLE] \
                = database_interface.DB_TARGET_SETTINGS[args.reference]

    @classmethod
    def get_target_env_base_url(cls):
        return cls._db_connection_data_pool['target']['base_url']

    @classmethod
    def get_db_connection(cls, env_role):

        if env_role not in cls._db_connection_pool:

            db_con_data = cls._db_connection_data_pool[env_role]
            print(f"Connecting to {env_role} database {db_con_data['db']}"
                  f" at {db_con_data['host']}:{db_con_data['port']}")
            db_connection = psycopg2.connect(
                host=db_con_data['host'],
                port=db_con_data['port'],
                user=db_con_data['user'],
                dbname=db_con_data['dbname'],
                password=db_con_data['passwd'],
            )

            db_connection.autocommit = database_interface.DB_AUTOCOMMIT
            db_connection.isolation_level = database_interface.DB_ISOLATION_LEVEL

            return db_connection

        else:
            return cls._db_connection_pool[env_role]

    @classmethod
    def process_subcommand(cls, args: argparse.Namespace):

        if args.subcommand[1] == database_interface.GET_COMPLETE_SCHEMA:
            db_data = database_interface.DB_ORIGIN_SETTINGS[args.origin]
            base = automap_base()
            engine_string = "postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{db}".format(**db_data)
            engine = create_engine(engine_string)

            def _gen_relationship(base, direction, return_fn,
                                  attrname, local_cls, referred_cls, **kw):
                return generate_relationship(base, direction, return_fn,
                                             attrname + '_ref', local_cls, referred_cls, **kw)

            base.prepare(autoload_with=engine, generate_relationship=_gen_relationship)
            session = Session(engine)

            # Use hook to pull origin entities and parse
            # session.query(base.classes.her_maphsa).first().id_maphsa
            raise NotImplemented()

        else:
            print(f"Unknown database_interface subcommand mode {args.subcommand[1]}")

    @classmethod
    def filter_concept_string(cls, cs: str) -> str:
        return cs.replace("'", "''")

    @classmethod
    def get_concept_id_mapping(cls, thesaurus_string: str, concept_string: str) -> int:
        concept_id_mappings = cls.get_concept_id_mappings()
        return concept_id_mappings[thesaurus_string][concept_string]

    @classmethod
    def get_concept_id_mappings(cls):

        if cls._concept_id_mappings is None:
            select_sources_string = open(f"{database_interface.DB_TEMPLATE_URL_PATH}/select_concepts.j2", 'r').read()
            select_sources_template = Template(select_sources_string)
            select_query = select_sources_template.render()

            cursor = cls.get_origin_db_connection().cursor()
            cursor.execute(select_query)
            result = cursor.fetchall()

            concept_id_mappings = {}
            for (concept_id, concept_string, thesaurus_string) in result:
                if thesaurus_string not in concept_id_mappings.keys():
                    concept_id_mappings[thesaurus_string] = {}
                concept_id_mappings[thesaurus_string][concept_string] = concept_id

            cls._concept_id_mappings = concept_id_mappings

        return cls._concept_id_mappings

    @classmethod
    def clean_insert_data(cls, target_data: dict) -> dict:
        for (k, v) in target_data.items():
            if type(v) is str:
                target_data[k] = str(v).replace("'", "''")

        return target_data

    @classmethod
    def run_script(cls, script_name: str, target_data: dict, target_db_connection):

        return_value = None

        target_data = cls.clean_insert_data(target_data)
        script_string = open(f"{database_interface.DB_SCRIPT_PATH}/{script_name}.j2", 'r').read()
        script_template = Template(script_string)
        script_query = script_template.render(target_data)
        curs = target_db_connection.cursor()
        try:
            curs.execute(script_query)
            return_value = curs.fetchall()

        except psycopg2.errors.DataError as e:
            raise (Exception(f"Error{e.pgcode}: {e.pgerror}\n{script_query}"))
        curs.close()

        if return_value:
            return return_value
        else:
            return


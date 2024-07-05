from arches_interface import arches_interface
from arches_interface import thesaurus_parser
from exceptions.exceptions import MAPHSARDMIntegrityException

EXPORT_TARGET_RDM_SUBCOMMAND = 'export_target_rdm'
VERIFY_TARGET_CC_INTEGRITY_SUBCOMMAND = 'verify_target_cc_integrity'
COMPARE_CONCEPT_COLLECTIONS_SUBCOMMAND = 'compare_concept_collections'
CROSS_VALIDATE_ORIGIN_CONCEPT_COLLECTIONS_SUBCOMMAND = "cv_origin_cc"
CROSS_VALIDATE_ONLINE_THESAURUS_CONCEPT_COLLECTIONS_SUBCOMMAND = "cv_online_thesaurus_cc"

arches_interface_subcommands = [EXPORT_TARGET_RDM_SUBCOMMAND, VERIFY_TARGET_CC_INTEGRITY_SUBCOMMAND,
                                COMPARE_CONCEPT_COLLECTIONS_SUBCOMMAND,
                                CROSS_VALIDATE_ORIGIN_CONCEPT_COLLECTIONS_SUBCOMMAND,
                                CROSS_VALIDATE_ONLINE_THESAURUS_CONCEPT_COLLECTIONS_SUBCOMMAND]

ARCHES_SYSTEM_COLLECTIONS = ['Arches', 'Candidates', 'Resource To Resource Relationship Types']

OUTPUT_PATH = 'output'


class ArchesConcept:

    def __init__(self, _id: str = "", _pref_label: str = "", _note: str = ""):
        self._id: str = _id
        self._pref_label: str = _pref_label
        self._note = _note

    @property
    def id(self):
        return self._id

    @property
    def pref_label(self):
        return self._pref_label

    @property
    def note(self):
        return self._note

    @note.setter
    def note(self, value):
        self._note = value


class ArchesConceptScheme:
    def __init__(self):
        self._id: str = ""
        self._title: str = ""
        self._members: dict = {}

    @property
    def members(self):
        return self._members

    @property
    def title(self):
        return self._title

    @property
    def id(self):
        return self._id


class ArchesConceptCollection:
    def __init__(self):
        self._id: str = ""
        self._title: str = ""
        self._members: dict = {}

    @property
    def members(self):
        return self._members

    @property
    def title(self):
        return self._title

    @property
    def id(self):
        return self._id


class ArchesConceptCollectionData:
    def __init__(self):
        self._concepts: dict = {}
        self._concept_schemes: dict = {}
        self._collections: dict = {}

    @property
    def concepts(self):
        return self._concepts

    @property
    def concept_schemes(self):
        return self._concept_schemes

    @concept_schemes.setter
    def concept_schemes(self, value):
        self._concept_schemes = value

    @property
    def collections(self):
        return self._collections

    @collections.setter
    def collections(self, value):
        self._collections = value

    def add_concept_scheme(self, concept_scheme: ArchesConceptScheme):
        self._concept_schemes[concept_scheme.title] = concept_scheme

        for c in concept_scheme.members.values():
            self.concepts[c.id] = c

    def add_concept_collection(self, concept_collection: ArchesConceptCollection):

        if concept_collection.title not in self._concept_schemes.keys():
            raise MAPHSARDMIntegrityException(f"{concept_collection.title} not present in concept schemes")

        concept_scheme: ArchesConceptScheme = self.concept_schemes[concept_collection.title]

        for c in concept_collection.members.values():
            if c.id not in concept_scheme.members.keys():
                raise MAPHSARDMIntegrityException(f"Concept {c.pref_label} from collection {concept_collection.title}"
                                                  f" not present in corresponding concept scheme")

        self._collections[concept_collection.title] = concept_collection




# MAPHSA Toolset

A general-purpose toolset that allows for data to be parsed, imported and exported across the MAPHSA environments, including raw csv/json, PostGIS, Postgres and Arches.


## Current Supported Commands

**Verify target RDM**: Ensure the target arches deployment has a healthy RDM that has matching concept schemes and collections.

```arches_interface export_target_rdm -t local```

**Cross-validate origin and target RDMs**: Check that an origin Postgis Arches deployment and a target Arches deployment share the same RDM. Otherwise, differences are printed.

```arches_interface cv_origin_cc -o local -t local```

**Cross-validate online thesaurus and target RDM**: Check that the online gdrive thesaurus and a target Arches deployment share the same RDM. Differences are printed.

```arches_interface cv_online_thesaurus_cc -t local```

**Export target RDM**: Export the target Arches deployment's RDM in XML RDF format.

```arches_interface export_target_rdm -t local```

## Update History

* 05/07/24 Initial commit with support for RDM features for RDF, gdrive, Postgis and Arches
import json
from decimal import Decimal, DecimalException
from urllib.parse import urlparse

import ckan.plugins.toolkit as toolkit
from rdflib import URIRef, BNode, Literal, Namespace
from ckanext.dcat.utils import resource_uri

from .base import URIRefOrLiteral, CleanedURIRef
from .base import (
    RDF,
    DCAT,
    DCATAP,
    DCT,
    XSD,
    SCHEMA,
    RDFS,
    ADMS,
)

from .euro_dcat_ap_base import BaseEuropeanDCATAPProfile


ELI = Namespace("http://data.europa.eu/eli/ontology#")


class EuropeanDCATAP2Profile(BaseEuropeanDCATAPProfile):
    """
    An RDF profile based on the DCAT-AP 2 for data portals in Europe

    More information and specification:

    https://joinup.ec.europa.eu/asset/dcat_application_profile

    """

    def parse_dataset(self, dataset_dict, dataset_ref):

        # Call base method for common properties
        dataset_dict = self._parse_dataset_base(dataset_dict, dataset_ref)

        # DCAT AP v2 properties also applied to higher versions
        dataset_dict = self._parse_dataset_v2(dataset_dict, dataset_ref)

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        # Call base method for common properties
        self._graph_from_dataset_base(dataset_dict, dataset_ref)

        # DCAT AP v2 properties also applied to higher versions
        self._graph_from_dataset_v2(dataset_dict, dataset_ref)

        # DCAT AP v2 specific properties
        self._graph_from_dataset_v2_only(dataset_dict, dataset_ref)

    def graph_from_catalog(self, catalog_dict, catalog_ref):

        self._graph_from_catalog_base(catalog_dict, catalog_ref)

    def _parse_dataset_v2(self, dataset_dict, dataset_ref):
        """
        DCAT -> CKAN properties carried forward to higher DCAT-AP versions
        """

        # Call base super method for common properties
        super().parse_dataset(dataset_dict, dataset_ref)

        # Standard values
        value = self._object_value(dataset_ref, DCAT.temporalResolution)
        if value:
            dataset_dict["extras"].append(
                {"key": "temporal_resolution", "value": value}
            )

        # Lists
        for key, predicate in (
            ("is_referenced_by", DCT.isReferencedBy),
            ("applicable_legislation", DCATAP.applicableLegislation),
            ("hvd_category", DCATAP.hvdCategory),
        ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                dataset_dict["extras"].append({"key": key, "value": json.dumps(values)})
        # Temporal
        start, end = self._time_interval(dataset_ref, DCT.temporal, dcat_ap_version=2)
        if start:
            self._insert_or_update_temporal(dataset_dict, "temporal_start", start)
        if end:
            self._insert_or_update_temporal(dataset_dict, "temporal_end", end)

        # Spatial
        spatial = self._spatial(dataset_ref, DCT.spatial)
        for key in ("bbox", "centroid"):
            self._add_spatial_to_dict(dataset_dict, key, spatial)

        # Spatial resolution in meters
        spatial_resolution = self._object_value_float_list(
            dataset_ref, DCAT.spatialResolutionInMeters
        )
        if spatial_resolution:
            # For some reason we incorrectly allowed lists in this property at
            # some point, keep support for it but default to single value
            value = (
                spatial_resolution[0]
                if len(spatial_resolution) == 1
                else json.dumps(spatial_resolution)
            )
            dataset_dict["extras"].append(
                {
                    "key": "spatial_resolution_in_meters",
                    "value": value,
                }
            )

        # Resources
        for distribution in self._distributions(dataset_ref):
            distribution_ref = str(distribution)
            for resource_dict in dataset_dict.get("resources", []):
                # Match distribution in graph and distribution in resource dict
                if resource_dict and distribution_ref == resource_dict.get(
                    "distribution_ref"
                ):
                    #  Simple values
                    for key, predicate in (
                        ("availability", DCATAP.availability),
                        ("compress_format", DCAT.compressFormat),
                        ("package_format", DCAT.packageFormat),
                        ("temporal_resolution", DCAT.temporalResolution),
                    ):
                        value = self._object_value(distribution, predicate)
                        if value:
                            resource_dict[key] = value

                    # Spatial resolution in meters
                    spatial_resolution = self._object_value_float_list(
                        distribution, DCAT.spatialResolutionInMeters
                    )
                    if spatial_resolution:
                        value = (
                            spatial_resolution[0]
                            if len(spatial_resolution) == 1
                            else json.dumps(spatial_resolution)
                        )
                        resource_dict["spatial_resolution_in_meters"] = value

                    #  Lists
                    for key, predicate in (
                        ("applicable_legislation", DCATAP.applicableLegislation),
                    ):
                        values = self._object_value_list(distribution, predicate)
                        if values:
                            resource_dict[key] = json.dumps(values)

                    # Access services
                    access_service_list = []

                    for access_service in self.g.objects(
                        distribution, DCAT.accessService
                    ):
                        access_service_dict = {}

                        #  Simple values
                        for key, predicate in (
                            ("availability", DCATAP.availability),
                            ("title", DCT.title),
                            ("endpoint_description", DCAT.endpointDescription),
                            ("license", DCT.license),
                            ("access_rights", DCT.accessRights),
                            ("description", DCT.description),
                        ):
                            value = self._object_value(access_service, predicate)
                            if value:
                                access_service_dict[key] = value
                        #  List
                        for key, predicate in (
                            ("endpoint_url", DCAT.endpointURL),
                            ("serves_dataset", DCAT.servesDataset),
                        ):
                            values = self._object_value_list(access_service, predicate)
                            if values:
                                access_service_dict[key] = values

                        # Access service URI (explicitly show the missing ones)
                        access_service_dict["uri"] = (
                            str(access_service)
                            if isinstance(access_service, URIRef)
                            else ""
                        )

                        # Remember the (internal) access service reference for
                        # referencing in further profiles, e.g. for adding more
                        # properties
                        access_service_dict["access_service_ref"] = str(access_service)

                        access_service_list.append(access_service_dict)

                    if access_service_list:
                        resource_dict["access_services"] = json.dumps(
                            access_service_list
                        )

        return dataset_dict

    def _graph_from_dataset_v2(self, dataset_dict, dataset_ref):
        """
        CKAN -> DCAT properties carried forward to higher DCAT-AP versions
        """

        # Standard values
        self._add_triple_from_dict(
            dataset_dict,
            dataset_ref,
            DCAT.temporalResolution,
            "temporal_resolution",
            _datatype=XSD.duration,
        )

        # Lists
        for key, predicate, fallbacks, type, datatype, _class in (
            (
                "is_referenced_by",
                DCT.isReferencedBy,
                None,
                URIRefOrLiteral,
                None,
                RDFS.Resource,
            ),
            (
                "applicable_legislation",
                DCATAP.applicableLegislation,
                None,
                URIRefOrLiteral,
                None,
                ELI.LegalResource,
            ),
            ("hvd_category", DCATAP.hvdCategory, None, URIRefOrLiteral, None, None),
        ):
            self._add_triple_from_dict(
                dataset_dict,
                dataset_ref,
                predicate,
                key,
                list_value=True,
                fallbacks=fallbacks,
                _type=type,
                _datatype=datatype,
                _class=_class,
            )

        # Temporal

        # The profile for DCAT-AP 1 stored triples using schema:startDate,
        # remove them to avoid duplication
        for temporal in self.g.objects(dataset_ref, DCT.temporal):
            if SCHEMA.startDate in [t for t in self.g.predicates(temporal, None)]:
                self.g.remove((temporal, None, None))
                self.g.remove((dataset_ref, DCT.temporal, temporal))

        start = self._get_dataset_value(dataset_dict, "temporal_start")
        end = self._get_dataset_value(dataset_dict, "temporal_end")
        if start or end:
            temporal_extent_dcat = BNode()

            self.g.add((temporal_extent_dcat, RDF.type, DCT.PeriodOfTime))
            if start:
                self._add_date_triple(temporal_extent_dcat, DCAT.startDate, start)
            if end:
                self._add_date_triple(temporal_extent_dcat, DCAT.endDate, end)
            self.g.add((dataset_ref, DCT.temporal, temporal_extent_dcat))

        # spatial
        spatial_bbox = self._get_dataset_value(dataset_dict, "spatial_bbox")
        spatial_cent = self._get_dataset_value(dataset_dict, "spatial_centroid")

        if spatial_bbox or spatial_cent:
            spatial_ref = self._get_or_create_spatial_ref(dataset_dict, dataset_ref)

            if spatial_bbox:
                self._add_spatial_value_to_graph(spatial_ref, DCAT.bbox, spatial_bbox)

            if spatial_cent:
                self._add_spatial_value_to_graph(
                    spatial_ref, DCAT.centroid, spatial_cent
                )

        # Spatial resolution in meters
        spatial_resolution_in_meters = self._read_list_value(
            self._get_dataset_value(dataset_dict, "spatial_resolution_in_meters")
        )
        if spatial_resolution_in_meters:
            for value in spatial_resolution_in_meters:
                try:
                    self.g.add(
                        (
                            dataset_ref,
                            DCAT.spatialResolutionInMeters,
                            Literal(Decimal(value), datatype=XSD.decimal),
                        )
                    )
                except (ValueError, TypeError, DecimalException):
                    self.g.add(
                        (dataset_ref, DCAT.spatialResolutionInMeters, Literal(value))
                    )

        # Resources
        for resource_dict in dataset_dict.get("resources", []):

            distribution_ref = CleanedURIRef(resource_uri(resource_dict))

            #  Simple values
            items = [
                ("availability", DCATAP.availability, None, URIRefOrLiteral),
                (
                    "compress_format",
                    DCAT.compressFormat,
                    None,
                    URIRefOrLiteral,
                    DCT.MediaType,
                ),
                (
                    "package_format",
                    DCAT.packageFormat,
                    None,
                    URIRefOrLiteral,
                    DCT.MediaType,
                ),
            ]

            self._add_triples_from_dict(resource_dict, distribution_ref, items)

            # Temporal resolution
            self._add_triple_from_dict(
                resource_dict,
                distribution_ref,
                DCAT.temporalResolution,
                "temporal_resolution",
                _datatype=XSD.duration,
            )

            # Spatial resolution in meters
            spatial_resolution_in_meters = self._read_list_value(
                self._get_resource_value(resource_dict, "spatial_resolution_in_meters")
            )
            if spatial_resolution_in_meters:
                for value in spatial_resolution_in_meters:
                    try:
                        self.g.add(
                            (
                                distribution_ref,
                                DCAT.spatialResolutionInMeters,
                                Literal(Decimal(value), datatype=XSD.decimal),
                            )
                        )
                    except (ValueError, TypeError, DecimalException):
                        self.g.add(
                            (
                                distribution_ref,
                                DCAT.spatialResolutionInMeters,
                                Literal(value),
                            )
                        )
            #  Lists
            items = [
                (
                    "applicable_legislation",
                    DCATAP.applicableLegislation,
                    None,
                    URIRefOrLiteral,
                    ELI.LegalResource,
                ),
            ]
            self._add_list_triples_from_dict(resource_dict, distribution_ref, items)

            # Access services
            access_service_list = resource_dict.get("access_services", [])
            if isinstance(access_service_list, str):
                try:
                    access_service_list = json.loads(access_service_list)
                except ValueError:
                    access_service_list = []

            updated_access_services = []

            for access_service_dict in access_service_list:

                enriched = self._enrich_access_service_from_data_service(
                    access_service_dict
                )

                if enriched:
                    access_service_node = BNode()
                    access_service_dict["access_service_ref"] = str(access_service_node)

                    self.g.add((distribution_ref, DCAT.accessService, access_service_node))
                    self.g.add((access_service_node, RDF.type, DCAT.DataService))

                    minimal_items = [
                        ("title", DCT.title, None, Literal),
                        ("description", DCT.description, None, Literal),
                    ]

                    self._add_triples_from_dict(
                        access_service_dict, access_service_node, minimal_items
                    )

                    endpoint_items = [
                        (
                            "endpoint_url",
                            DCAT.endpointURL,
                            None,
                            URIRefOrLiteral,
                            RDFS.Resource,
                        ),
                    ]
                    self._add_list_triples_from_dict(
                        access_service_dict, access_service_node, endpoint_items
                    )

                    updated_access_services.append(access_service_dict)

                else:
                    uri = (access_service_dict.get("uri") or "").strip()
                    if not uri:
                        continue

                    access_service_node = CleanedURIRef(uri)
                    self.g.add((distribution_ref, DCAT.accessService, access_service_node))

                    updated_access_services.append({"uri": uri})

            if updated_access_services:
                resource_dict["access_services"] = json.dumps(updated_access_services)
            elif resource_dict.get("access_services"):
                resource_dict.pop("access_services", None)

    def _enrich_access_service_from_data_service(self, access_service_dict):
        """
        Populate access service metadata from a referenced data-service dataset, if available.
        """
        if not isinstance(access_service_dict, dict):
            return False

        def has_details():
            if access_service_dict.get("title") or access_service_dict.get("description"):
                return True
            urls = self._ensure_list(access_service_dict.get("endpoint_url"))
            return bool(urls)

        if has_details():
            return True

        data_service_id = self._resolve_data_service_identifier(access_service_dict)
        if not data_service_id:
            return False

        try:
            package_show = toolkit.get_action("package_show")
            data_service = package_show({"ignore_auth": True}, {"id": data_service_id})
        except (toolkit.ObjectNotFound, toolkit.NotAuthorized, toolkit.ValidationError):
            return False
        except Exception:
            # Any other unexpected error should not break serialization
            return False

        if data_service.get("type") != "data-service":
            return False

        def _translated_value(field, default_field=None):
            translated = data_service.get(f"{field}_translated")
            if isinstance(translated, dict):
                value = translated.get(self._default_lang)
                if not value:
                    for candidate in translated.values():
                        if candidate:
                            value = candidate
                            break
            else:
                value = None
            if not value:
                key = default_field or field
                value = data_service.get(key)
            return value

        if not access_service_dict.get("title"):
            title = _translated_value("title")
            if title:
                access_service_dict["title"] = title

        if not access_service_dict.get("description"):
            description = _translated_value("notes")
            if description:
                access_service_dict["description"] = description

        if not access_service_dict.get("endpoint_url"):
            urls = self._ensure_list(data_service.get("endpoint_url"))
            if urls:
                access_service_dict["endpoint_url"] = urls

        return has_details()

    @staticmethod
    def _ensure_list(value):
        if not value:
            return []
        if isinstance(value, list):
            return [item for item in value if item]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except ValueError:
                parsed = None
            if isinstance(parsed, list):
                return [item for item in parsed if item]
            return [value]
        return [value]

    @staticmethod
    def _resolve_data_service_identifier(access_service_dict):
        """
        Try to resolve a data service identifier from the access service metadata.
        """
        candidate_keys = ("data_service_id", "data_service", "data_service_slug")
        for key in candidate_keys:
            value = access_service_dict.get(key)
            if value:
                return value

        uri = access_service_dict.get("uri")
        if not uri or not isinstance(uri, str):
            return None

        uri = uri.strip()
        if not uri:
            return None

        parsed = urlparse(uri)
        if parsed.scheme and parsed.netloc:
            segments = [segment for segment in parsed.path.split("/") if segment]
            if not segments:
                return None
            if "data-service" in segments:
                idx = segments.index("data-service")
                if idx + 1 < len(segments):
                    return segments[idx + 1]
            if "data-services" in segments:
                idx = segments.index("data-services")
                if idx + 1 < len(segments):
                    return segments[idx + 1]
            return segments[-1]

        return uri or None

    def _graph_from_dataset_v2_only(self, dataset_dict, dataset_ref):
        """
        CKAN -> DCAT v2 specific properties (not applied to higher versions)
        """

        # Other identifiers (these are handled differently in the
        # DCAT-AP v3 profile)
        self._add_triple_from_dict(
            dataset_dict,
            dataset_ref,
            ADMS.identifier,
            "alternate_identifier",
            list_value=True,
            _type=URIRefOrLiteral,
            _class=ADMS.Identifier,
        )

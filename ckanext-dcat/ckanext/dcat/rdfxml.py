"""Εξειδικευμένη RDF/XML σειριοποίηση για DCAT output.

Το rdflib γράφει επαναλαμβανόμενα URIRef αντικείμενα ως rdf:resource. Για το
DCAT-AP θέλουμε το dct:format με dct:MediaTypeOrExtent να εμφανίζεται inline
στο RDF/XML, όπως τα υπόλοιπα controlled vocabulary πεδία.
"""

from io import BytesIO

from rdflib import Namespace, URIRef
from rdflib.namespace import RDF
from rdflib.plugins.parsers.RDFVOC import RDFVOC
from rdflib.plugins.serializers.rdfxml import PrettyXMLSerializer


DCT = Namespace("http://purl.org/dc/terms/")
RDF_XML_MAX_DEPTH = 3


class DCATPrettyXMLSerializer(PrettyXMLSerializer):
    def predicate(self, predicate, obj, depth=1):
        if self._should_inline_media_type_or_extent_format(predicate, obj):
            self._inline_media_type_or_extent_format(predicate, obj, depth)
            return

        super(DCATPrettyXMLSerializer, self).predicate(predicate, obj, depth)

    def _should_inline_media_type_or_extent_format(self, predicate, obj):
        return (
            predicate == DCT["format"]
            and isinstance(obj, URIRef)
            and (obj, RDF.type, DCT.MediaTypeOrExtent) in self.store
        )

    def _inline_media_type_or_extent_format(self, predicate, obj, depth):
        writer = self.writer
        writer.push(predicate)
        writer.push(DCT.MediaTypeOrExtent)
        writer.attribute(RDFVOC.about, self.relativize(obj))

        self._PrettyXMLSerializer__serialized[obj] = 1
        for child_predicate, child_obj in self.store.predicate_objects(obj):
            if (
                child_predicate == RDF.type
                and child_obj == DCT.MediaTypeOrExtent
            ):
                continue
            self.predicate(child_predicate, child_obj, depth + 1)

        writer.pop(DCT.MediaTypeOrExtent)
        writer.pop(predicate)


def serialize_dcat_pretty_xml(graph):
    stream = BytesIO()
    serializer = DCATPrettyXMLSerializer(graph)
    serializer.serialize(
        stream,
        base=graph.base,
        encoding="utf-8",
        max_depth=RDF_XML_MAX_DEPTH,
    )
    return stream.getvalue().decode("utf-8")

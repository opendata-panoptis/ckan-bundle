# TODO: Needs to load the vocabulary service for 'nature of relationship' and parse it into forward/reverse relationship types somehow before overriding the PackageRelationship.types variable
# Implement after this story has completed https://it-partners.atlassian.net/browse/DDCI-40
RELATIONSHIP_TYPES = [
    (u'unspecified relationship', u'unspecified relationship'),
    (u'Is Part Of', u'Has Part'),
    (u'Is Format Of', u'Has Format'),
    (u'Is Version Of', u'Has Version'),
    (u'Replaces', u'Is Replaced By'),
    (u'References', u'Is Referenced By'),
    (u'Requires', u'Is Required By'),
    (u'Was Derived From', u'Was Derivation of'),
    (u'Was Influenced By', u'Influenced'),
    (u'Quoted As', u'Quoted From'),
    (u'Was Revision Of', u'Had Revision'),
    (u'Is Alternate Of', u'Is Alternate Of'),
    (u'Is Specialization Of', u'Is Generalization Of'),
]

RELATIONSHIP_TYPE_URIS = {
    'unspecified relationship': None,
    'Is Part Of': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/isPartOf',
    'Has Part': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/hasPart',
    'Is Format Of': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/isFormatOf',
    'Has Format': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/hasFormat',
    'Is Version Of': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/isVersionOf',
    'Has Version': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/hasVersion',
    'Replaces': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/replaces',
    'Is Replaced By': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/isReplacedBy',
    'References': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/references',
    'Is Referenced By': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/isReferencedBy',
    'Requires': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/requires',
    'Is Required By': 'https://vocabs.gsq.digital/object?uri=http%3A//purl.org/dc/terms/isRequiredBy',
    'Was Derived From': 'https://vocabs.gsq.digital/object?uri=http%3A//www.w3.org/ns/prov/%23wasDerivedFrom',
    'Was Derivation of': 'https://vocabs.gsq.digital/object?uri=https%3A//linked.data.gov.au/def/dataset-relationships/hadDerivation',
    'Was Influenced By': 'https://vocabs.gsq.digital/object?uri=http%3A//www.w3.org/ns/prov/%23wasInfluencedBy',
    'Influenced': 'https://vocabs.gsq.digital/object?uri=http%3A//www.w3.org/ns/prov/%23influenced',
    'Quoted As': 'https://vocabs.gsq.digital/object?uri=https%3A//linked.data.gov.au/def/dataset-relationships/quotedAs',
    'Quoted From': 'https://vocabs.gsq.digital/object?uri=http%3A//www.w3.org/ns/prov/%23wasQuotedFrom',
    'Was Revision Of': 'https://vocabs.gsq.digital/object?uri=http%3A//www.w3.org/ns/prov/%23wasRevisionOf',
    'Had Revision': 'https://vocabs.gsq.digital/object?uri=https%3A//linked.data.gov.au/def/dataset-relationships/hadRevision',
    'Is Alternate Of': 'https://vocabs.gsq.digital/object?uri=http%3A//www.w3.org/ns/prov/%23alternateOf',
    'Is Specialization Of': 'https://vocabs.gsq.digital/object?uri=http%3A//www.w3.org/ns/prov/%23specializationOf',
    'Is Generalization Of': 'https://vocabs.gsq.digital/object?uri=https%3A//linked.data.gov.au/def/dataset-relationships/generalizationOf'
}

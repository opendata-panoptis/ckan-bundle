
"""
Some very thin wrapper classes around those in OWSLib
for convenience.
"""
import logging
from urllib.parse import urlencode, urlparse, urlunparse

from owslib.etree import etree
from owslib.fes import PropertyIsEqualTo, SortBy, SortProperty

log = logging.getLogger(__name__)

class CswError(Exception):
    pass

class OwsService(object):
    def __init__(self, endpoint=None):
        if endpoint is not None:
            self._ows(endpoint)

    def __call__(self, args):
        return getattr(self, args.operation)(**self._xmd(args))

    @classmethod
    def _operations(cls):
        return [x for x in dir(cls) if not x.startswith("_")]

    def _xmd(self, obj):
        md = {}
        for attr in [x for x in dir(obj) if not x.startswith("_")]:
            val = getattr(obj, attr)
            if not val:
                pass
            elif callable(val):
                pass
            elif isinstance(val, str):
                md[attr] = val
            elif isinstance(val, int):
                md[attr] = val
            elif isinstance(val, list):
                md[attr] = val
            else:
                md[attr] = self._xmd(val)
        return md

    def _ows(self, endpoint=None, **kw):
        if not hasattr(self, "_Implementation"):
            raise NotImplementedError("Needs an Implementation")
        if not hasattr(self, "__ows_obj__"):
            if endpoint is None:
                raise ValueError("Must specify a service endpoint")
            self.__ows_obj__ = self._Implementation(endpoint)
        return self.__ows_obj__

    def getcapabilities(self, debug=False, **kw):
        ows = self._ows(**kw)
        caps = self._xmd(ows)
        if not debug:
            if "request" in caps: del caps["request"]
            if "response" in caps: del caps["response"]
        if "owscommon" in caps: del caps["owscommon"]
        return caps

class CswService(OwsService):
    """
    Perform various operations on a CSW service
    """
    from owslib.catalogue.csw2 import CatalogueServiceWeb as _Implementation

    def __init__(self, endpoint=None):
        super(CswService, self).__init__(endpoint)
        self.sortby = SortBy([SortProperty('dc:identifier')])

    def getrecords(self, qtype=None, keywords=[],
                   typenames="csw:Record", esn="brief",
                   skip=0, count=10, outputschema="gmd", **kw):
        from owslib.catalogue.csw2 import namespaces
        constraints = []
        csw = self._ows(**kw)

        if qtype is not None:
           constraints.append(PropertyIsEqualTo("dc:type", qtype))

        kwa = {
            "constraints": constraints,
            "typenames": typenames,
            "esn": esn,
            "startposition": skip,
            "maxrecords": count,
            "outputschema": namespaces[outputschema],
            "sortby": self.sortby
            }
        log.info('Making CSW request: getrecords2 %r', kwa)
        csw.getrecords2(**kwa)
        if csw.exceptionreport:
            err = 'Error getting records: %r' % \
                  csw.exceptionreport.exceptions
            #log.error(err)
            raise CswError(err)
        return [self._xmd(r) for r in list(csw.records.values())]

    def getidentifiers(self, qtype=None, typenames="gmd:MD_Metadata", esn="brief",
                       keywords=[], limit=None, page=10, outputschema="gmd",
                       startposition=0, cql=None, **kw):
        from owslib.catalogue.csw2 import namespaces
        constraints = []
        csw = self._ows(**kw)

        if qtype is not None:
            constraints.append(PropertyIsEqualTo("dc:type", qtype))

        kwa = {
            "constraints": constraints,
            "typenames": typenames,
            "esn": esn,
            "startposition": startposition,
            "maxrecords": page,
            "outputschema": namespaces[outputschema],
            "cql": cql,
            "sortby": self.sortby
        }
        i = 0
        matches = 0

        # ΠΡΟΣΘΗΚΗ: Προσπάθεια με POST πρώτα, μετά με GET αν αποτύχει
        use_get_method = False

        while True:
            if use_get_method:
                # Χρήση GET method
                identifiers = list(self._getidentifiers_using_get(
                    qtype, typenames, esn, keywords, page, outputschema,
                    startposition, cql, **kw
                ))
                # ΠΡΟΣΘΗΚΗ: Για GET method, χρησιμοποιούμε το matches από την πρώτη κλήση
                if matches == 0 and identifiers:
                    # Κάνουμε ένα initial request για να πάρουμε το συνολικό αριθμό εγγραφών
                    try:
                        initial_params = {
                            'service': 'CSW',
                            'version': '2.0.2',
                            'request': 'GetRecords',
                            'typeNames': typenames,
                            'resultType': 'hits',  # Μόνο για μέτρηση
                            'outputSchema': 'http://www.isotc211.org/2005/gmd'
                        }
                        if cql:
                            initial_params['constraintLanguage'] = 'CQL_TEXT'
                            initial_params['constraint'] = cql

                        import requests
                        from urllib.parse import urlencode, urlparse, urlunparse
                        url_parts = urlparse(self._ows(**kw).url)
                        url = urlunparse(
                            (url_parts.scheme, url_parts.netloc, url_parts.path, '', urlencode(initial_params), ''))

                        response = requests.get(url, verify=False, timeout=30)
                        response.raise_for_status()

                        from lxml import etree as lxml_etree
                        root = lxml_etree.fromstring(response.content)
                        ns = {'csw': 'http://www.opengis.net/cat/csw/2.0.2'}
                        matches_elem = root.xpath('//csw:SearchResults/@numberOfRecordsMatched', namespaces=ns)
                        if matches_elem:
                            matches = int(matches_elem[0])
                            log.info('Total records found via GET: %d', matches)
                    except Exception as e:
                        log.warning('Could not get total records count: %s', str(e))
                        matches = 1000000  # Μεγάλος αριθμός για safety
            else:
                # Χρήση POST method (προεπιλογή)
                try:
                    log.info('Making CSW POST request: getrecords2 %r', kwa)
                    csw.getrecords2(**kwa)

                    # Έλεγχος για HTML response (σφάλμα)
                    if hasattr(csw, 'response'):
                        response_text = csw.response.decode('utf-8') if isinstance(csw.response, bytes) else str(
                            csw.response)
                        if '<!DOCTYPE html' in response_text or '<html' in response_text.lower():
                            log.warning('CSW server returned HTML instead of XML, switching to GET method')
                            use_get_method = True
                            identifiers = list(self._getidentifiers_using_get(
                                qtype, typenames, esn, keywords, page, outputschema,
                                startposition, cql, **kw
                            ))
                            continue  # Συνεχίζουμε με GET method
                        else:
                            if csw.exceptionreport:
                                err = 'Error getting identifiers: %r' % \
                                      csw.exceptionreport.exceptions
                                log.error(err)
                                raise CswError(err)

                            if matches == 0:
                                matches = csw.results['matches']

                            identifiers = list(csw.records.keys())
                except Exception as e:
                    log.warning('POST request failed, switching to GET method: %s', str(e))
                    use_get_method = True
                    identifiers = list(self._getidentifiers_using_get(
                        qtype, typenames, esn, keywords, page, outputschema,
                        startposition, cql, **kw
                    ))
                    continue  # Συνεχίζουμε με GET method

            # Ελέγχουμε αν έχουμε identifiers
            if not identifiers:
                break

            if limit is not None:
                identifiers = identifiers[:(limit - startposition)]

            for ident in identifiers:
                yield ident

            i += len(identifiers)

            # Έλεγχος ορίων
            if limit is not None and i >= limit:
                break

            if use_get_method and matches > 0 and startposition + page >= matches:
                break

            startposition += page

            # Έλεγχος για POST method
            if not use_get_method and startposition >= matches:
                break

            kwa["startposition"] = startposition

    def _getidentifiers_using_get(self, qtype=None, typenames="gmd:MD_Metadata", esn="brief",
                                  keywords=[], page=10, outputschema="gmd",
                                  startposition=0, cql=None, **kw):
        """Alternative implementation using GET requests instead of POST"""
        import requests
        from lxml import etree as lxml_etree

        # Βασικά parameters
        output_schema_url = {
            'gmd': 'http://www.isotc211.org/2005/gmd',
            'csw': 'http://www.opengis.net/cat/csw/2.0.2'
        }.get(outputschema, 'http://www.isotc211.org/2005/gmd')

        params = {
            'service': 'CSW',
            'version': '2.0.2',
            'request': 'GetRecords',
            'typeNames': typenames,
            'resultType': 'results',
            'outputSchema': output_schema_url,
            'elementSetName': esn,
            'startPosition': startposition + 1,
            'maxRecords': page
        }

        # Προσθήκη CQL filter αν υπάρχει
        if cql:
            params['constraintLanguage'] = 'CQL_TEXT'
            params['constraint'] = cql

        url_parts = urlparse(self._ows(**kw).url)
        url = urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, '', urlencode(params), ''))

        log.info('Making CSW GET request: %s', url)

        try:
            # Απενεργοποίηση SSL verification
            response = requests.get(url, verify=False, timeout=30)
            response.raise_for_status()

            # Parse XML response
            root = lxml_etree.fromstring(response.content)

            # ΠΡΟΣΘΗΚΗ: Έλεγχος για exceptions
            namespaces = {
                'csw': 'http://www.opengis.net/cat/csw/2.0.2',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'gmd': 'http://www.isotc211.org/2005/gmd',
                'gco': 'http://www.isotc211.org/2005/gco',
                'dct': 'http://purl.org/dc/terms/',
                'ows': 'http://www.opengis.net/ows'
            }

            # Έλεγχος αν η απάντηση είναι ExceptionReport
            exception = root.xpath('//ows:Exception', namespaces=namespaces)
            if exception:
                exception_text = root.xpath('//ows:ExceptionText/text()', namespaces=namespaces)
                if exception_text:
                    log.warning('CSW server returned exception: %s', exception_text[0])
                    return []  # Επιστροφή κενής λίστας για να σταματήσει το pagination

            # Extract identifiers
            identifiers = []

            # Query 1: Για csw:SummaryRecord με dc:identifier
            identifiers = root.xpath('//csw:SummaryRecord/dc:identifier/text()', namespaces=namespaces)

            # Query 2: Για csw:Record με dc:identifier
            if not identifiers:
                identifiers = root.xpath('//csw:Record/dc:identifier/text()', namespaces=namespaces)

            # Query 3: Για gmd:MD_Metadata με gmd:fileIdentifier
            if not identifiers:
                identifiers = root.xpath('//gmd:fileIdentifier/gco:CharacterString/text()', namespaces=namespaces)

            # Query 4: Οποιοδήποτε identifier
            if not identifiers:
                identifiers = root.xpath('//*[local-name()="identifier"]/text()', namespaces=namespaces)

            log.info('Found %d identifiers via GET request', len(identifiers))

            for identifier in identifiers:
                yield identifier

        except Exception as e:
            log.error('Error in GET-based identifier retrieval: %s', str(e))
            log.error('URL used: %s', url)
            raise CswError(f'GET-based retrieval failed: {str(e)}')

    def getrecordbyid(self, ids=[], esn="full", outputschema="gmd", **kw):
        from owslib.catalogue.csw2 import namespaces
        csw = self._ows(**kw)
        kwa = {
            "esn": esn,
            "outputschema": namespaces[outputschema],
            }
        # Ordinary Python version's don't support the metadata argument
        log.info('Making CSW request: getrecordbyid %r %r', ids, kwa)
        csw.getrecordbyid(ids, **kwa)
        if csw.exceptionreport:
            err = 'Error getting record by id: %r' % \
                  csw.exceptionreport.exceptions
            #log.error(err)
            raise CswError(err)
        if not csw.records:
            return
        record = self._xmd(list(csw.records.values())[0])

        ## strip off the enclosing results container, we only want the metadata
        #md = csw._exml.find("/gmd:MD_Metadata")#, namespaces=namespaces)
        # Ordinary Python version's don't support the metadata argument
        md = csw._exml.find("/{http://www.isotc211.org/2005/gmd}MD_Metadata")
        mdtree = etree.ElementTree(md)
        try:
            record["xml"] = etree.tostring(mdtree, pretty_print=True, encoding=str)
        except TypeError:
            # API incompatibilities between different flavours of elementtree
            try:
                record["xml"] = etree.tostring(mdtree, pretty_print=True, encoding=str)
            except AssertionError:
                record["xml"] = etree.tostring(md, pretty_print=True, encoding=str)

        record["xml"] = '<?xml version="1.0" encoding="UTF-8"?>\n' + record["xml"]
        record["tree"] = mdtree
        return record
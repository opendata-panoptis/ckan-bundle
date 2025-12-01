import re
from urllib.parse import urlparse, urlunparse, urlencode

import logging

from ckan import model

from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from ckanext.spatial.lib.csw_client import CswService
from ckanext.spatial.harvesters.base import SpatialHarvester, text_traceback


class CSWHarvester(SpatialHarvester, SingletonPlugin):
    '''
    A Harvester for CSW servers
    '''
    implements(IHarvester)

    csw = None

    def info(self):
        return {
            'name': 'csw',
            'title': 'CSW Server',
            'description': 'A server that implements OGC\'s Catalog Service for the Web (CSW) standard'
        }

    def get_original_url(self, harvest_object_id):
        obj = model.Session.query(HarvestObject). \
            filter(HarvestObject.id == harvest_object_id). \
            first()

        parts = urlparse(obj.source.url)

        params = {
            'SERVICE': 'CSW',
            'VERSION': '2.0.2',
            'REQUEST': 'GetRecordById',
            'OUTPUTSCHEMA': 'http://www.isotc211.org/2005/gmd',
            'OUTPUTFORMAT': 'application/xml',
            'ID': obj.guid
        }

        url = urlunparse((
            parts.scheme,
            parts.netloc,
            parts.path,
            None,
            urlencode(params),
            None
        ))

        return url

    def output_schema(self):
        return 'gmd'

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.CSW.gather')
        log.debug('CswHarvester gather_stage for job: %r', harvest_job)
        # Get source URL
        url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        try:
            self._setup_csw_client(url)
        except Exception as e:
            self._save_gather_error('Error contacting the CSW server: %s' % e, harvest_job)
            return None

        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id). \
            filter(HarvestObject.current == True). \
            filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())

        # extract cql filter if any
        cql = self.source_config.get('cql')

        log.debug('Starting gathering for %s' % url)
        guids_in_harvest = set()
        try:
            for identifier in self.csw.getidentifiers(page=10, outputschema=self.output_schema(), cql=cql):
                try:
                    # ΠΡΟΣΘΗΚΗ: Έλεγχος για invalid identifiers
                    if identifier is None or 'Start position' in identifier or 'greater than' in identifier:
                        log.error('Invalid identifier %r, skipping...' % identifier)
                        continue

                    log.info('Got identifier %s from the CSW', identifier)
                    guids_in_harvest.add(identifier)
                except Exception as e:
                    self._save_gather_error('Error for the identifier %s [%r]' % (identifier, e), harvest_job)
                    continue

        except Exception as e:
            log.error('Exception: %s' % text_traceback())
            self._save_gather_error('Error gathering the identifiers from the CSW server [%s]' % str(e), harvest_job)
            return None

        new = guids_in_harvest - guids_in_db
        delete = guids_in_db - guids_in_harvest
        change = guids_in_db & guids_in_harvest

        ids = []
        for guid in new:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                extras=[HOExtra(key='status', value='new')])
            obj.save()
            ids.append(obj.id)
        for guid in change:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='change')])
            obj.save()
            ids.append(obj.id)
        for guid in delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='delete')])
            model.Session.query(HarvestObject). \
                filter_by(guid=guid). \
                update({'current': False}, False)
            obj.save()
            ids.append(obj.id)

        if len(ids) == 0:
            self._save_gather_error('No records received from the CSW server', harvest_job)
            return None

        return ids

    def fetch_stage(self, harvest_object):

        # Check harvest object status
        status = self._get_object_extra(harvest_object, 'status')

        if status == 'delete':
            # No need to fetch anything, just pass to the import stage
            return True

        log = logging.getLogger(__name__ + '.CSW.fetch')
        log.debug('CswHarvester fetch_stage for object: %s', harvest_object.id)

        url = harvest_object.source.url
        try:
            self._setup_csw_client(url)
        except Exception as e:
            self._save_object_error('Error contacting the CSW server: %s' % e,
                                    harvest_object)
            return False

        identifier = harvest_object.guid

        # ΠΡΟΣΘΗΚΗ: Έλεγχος αν το identifier είναι πραγματικό GUID ή error message
        if 'Start position' in identifier or 'greater than' in identifier:
            log.warning('Skipping invalid identifier (error message): %s', identifier)
            self._save_object_error('Invalid identifier (error message): %s' % identifier, harvest_object)
            return False

        try:
            # ΠΡΟΣΘΗΚΗ: Χρήση GET method για getrecordbyid αν το POST αποτύχει
            record = None
            try:
                # Πρώτη προσπάθεια: POST method
                record = self.csw.getrecordbyid([identifier], outputschema=self.output_schema())
            except Exception as post_error:
                log.warning('POST getrecordbyid failed, trying GET method: %s', str(post_error))
                # Δεύτερη προσπάθεια: GET method
                record = self._get_record_by_id_using_get(identifier, self.output_schema())

            if record is None:
                self._save_object_error('Empty record for GUID %s' % identifier, harvest_object)
                return False

            try:
                # Save the fetch contents in the HarvestObject
                # Contents come from csw_client already declared and encoded as utf-8
                # Remove original XML declaration
                content = re.sub('<\?xml(.*)\?>', '', record['xml'])
                harvest_object.content = content.strip()
                harvest_object.save()
            except Exception as e:
                self._save_object_error('Error saving the harvest object for GUID %s [%r]' % \
                                        (identifier, e), harvest_object)
                return False

            log.debug('XML content saved (len %s)', len(record['xml']))
            return True

        except Exception as e:
            self._save_object_error('Error getting the CSW record with GUID %s' % identifier, harvest_object)
            return False

    # ΠΡΟΣΘΗΚΗ: Νέα μέθοδος για GET-based getrecordbyid
    def _get_record_by_id_using_get(self, identifier, outputschema="gmd"):
        """Alternative implementation using GET requests for getrecordbyid"""
        import requests
        from urllib.parse import urlencode, urlparse, urlunparse
        from lxml import etree as lxml_etree

        log = logging.getLogger(__name__ + '.CSW.fetch')

        # Βασικά parameters
        output_schema_url = {
            'gmd': 'http://www.isotc211.org/2005/gmd',
            'csw': 'http://www.opengis.net/cat/csw/2.0.2'
        }.get(outputschema, 'http://www.isotc211.org/2005/gmd')

        params = {
            'service': 'CSW',
            'version': '2.0.2',
            'request': 'GetRecordById',
            'id': identifier,
            'outputSchema': output_schema_url,
            'elementSetName': 'full'
        }

        url_parts = urlparse(self.csw._ows().url)
        url = urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, '', urlencode(params), ''))

        log.info('Making CSW GET request for record: %s', url)

        try:
            response = requests.get(url, verify=False, timeout=30)
            response.raise_for_status()

            # Parse XML response
            root = lxml_etree.fromstring(response.content)

            # Extract the metadata record
            namespaces = {
                'gmd': 'http://www.isotc211.org/2005/gmd',
                'gco': 'http://www.isotc211.org/2005/gco',
                'csw': 'http://www.opengis.net/cat/csw/2.0.2'
            }

            # Ψάχνουμε για το MD_Metadata element
            md_element = root.find('.//{http://www.isotc211.org/2005/gmd}MD_Metadata')
            if md_element is None:
                # Αν δεν βρεθεί, δοκιμάζουμε άλλους paths
                md_element = root.find('.//MD_Metadata')
                if md_element is None:
                    md_element = root

            # Δημιουργία record dictionary
            record = {
                'xml': lxml_etree.tostring(md_element, pretty_print=True, encoding='unicode'),
                'tree': lxml_etree.ElementTree(md_element)
            }

            # Προσθήκη XML declaration
            record['xml'] = '<?xml version="1.0" encoding="UTF-8"?>\n' + record['xml']

            log.debug('Successfully retrieved record via GET for ID: %s', identifier)
            return record

        except Exception as e:
            log.error('GET getrecordbyid failed for ID %s: %s', identifier, str(e))
            return None

    def _setup_csw_client(self, url):
        self.csw = CswService(url)
import tempfile
import zipfile
import os
import hashlib
import math
import copy
import time

import requests
import six
import ckanapi
import ckanapi.datapackage

from ckan import model
from ckan.plugins.toolkit import get_action, config

from . import helpers


log = __import__('logging').getLogger(__name__)


def update_zip(package_id, user, skip_if_no_changes=True):
    '''
    Create/update the a dataset's zip resource, containing the other resources
    and some metadata.

    :param skip_if_no_changes: If true, and there is an existing zip for this
        dataset, it will compare a freshly generated package.json against what
        is in the existing zip, and if there are no changes (ignoring the
        Download All Zip) then it will skip downloading the resources and
        updating the zip.
    :param user: The username of the authenticated user who initiated the task.
        This is used to ensure the background task has the same permissions as the user.
    '''
    try:
        # Include the user in the context to handle private datasets
        context = {'model': model, 'session': model.Session}
        if user:
            context['user'] = user
        else:
            # When no user is provided (background job), use ignore_auth to bypass authorization
            context['ignore_auth'] = True
        dataset = get_action('package_show')(context, {'id': package_id})

        # Skip dataset types that should not have a download-all ZIP
        if helpers.is_data_service(dataset) or dataset.get('type') == 'showcase':
            log.info(
                'Skipping zip creation for dataset type "{}": {} (ID: {})'.format(
                    dataset.get('type', 'unknown'), dataset.get('name', 'unknown'), package_id))
            return

        log.info('Starting zip update for dataset: {} (ID: {})'.format(dataset['name'], package_id))

        datapackage, ckan_and_datapackage_resources, existing_zip_resource = \
            generate_datapackage_json(package_id, user)

        log.info('Generated datapackage with {} resources'.format(len(ckan_and_datapackage_resources)))

        if existing_zip_resource:
            log.info('Found existing zip resource: {}'.format(existing_zip_resource.get('id', 'unknown')))
        else:
            log.info('No existing zip resource found')

        # For the first creation of a dataset or when resources are added to a dataset that previously had no resources,
        # we want to force the creation of the zip file, regardless of whether the datapackage has changed significantly
        force_update = False
        if len(ckan_and_datapackage_resources) > 0 and (not existing_zip_resource or
                                                       existing_zip_resource.get('size', 0) == 0):
            log.info('Forcing zip update because dataset has resources but no existing zip or empty zip')
            force_update = True

        if not force_update and skip_if_no_changes and existing_zip_resource and \
                not has_datapackage_changed_significantly(
                    datapackage, ckan_and_datapackage_resources,
                    existing_zip_resource):
            log.info('Skipping updating the zip - the datapackage.json is not '
                    'changed sufficiently: {}'.format(dataset['name']))
            return

        prefix = "{}-".format(dataset[u'name'])
        with tempfile.NamedTemporaryFile(prefix=prefix, suffix='.zip') as fp:
            filesize = write_zip(fp, datapackage, ckan_and_datapackage_resources, user=user)
            log.info('Zip file created with size: {} bytes'.format(filesize))

            # Rewind the file pointer to the beginning of the file
            fp.seek(0)

            # Upload resource to CKAN as a new/updated resource
            # Use the authenticated user for the LocalCKAN instance if available
            local_ckan = ckanapi.LocalCKAN(username=user) if user else ckanapi.LocalCKAN()
            fp.seek(0)
            resource = dict(
                package_id=dataset['id'],
                url='dummy-value',
                upload=fp,
                name=u'All resource data',
                format=u'ZIP',
                downloadall_metadata_modified=dataset['metadata_modified'],
                downloadall_datapackage_hash=hash_datapackage(datapackage)
            )

            if not existing_zip_resource:
                log.info('Creating new zip resource for dataset: {}'.format(dataset['name']))
                result = local_ckan.action.resource_create(**resource)
                log.info('New zip resource created with ID: {}'.format(result.get('id', 'unknown')))
            else:
                log.info('Updating existing zip resource ID: {}'.format(existing_zip_resource['id']))
                result = local_ckan.action.resource_patch(
                    id=existing_zip_resource['id'],
                    **resource)
                log.info('Zip resource updated successfully')
    except Exception as e:
        log.error('Error in update_zip for dataset {}: {}'.format(package_id, str(e)))
        import traceback
        log.error(traceback.format_exc())
        raise


class DownloadError(Exception):
    """Exception raised when a resource cannot be downloaded."""
    def __init__(self, message="Resource download failed"):
        self.message = message
        super(DownloadError, self).__init__(self.message)


def has_datapackage_changed_significantly(
        datapackage, ckan_and_datapackage_resources, existing_zip_resource):
    '''Compare the freshly generated datapackage with the existing one and work
    out if it is changed enough to warrant regenerating the zip.

    :returns bool: True if the data package has really changed and needs
        regenerating
    '''
    assert existing_zip_resource
    new_hash = hash_datapackage(datapackage)
    old_hash = existing_zip_resource.get('downloadall_datapackage_hash')
    return new_hash != old_hash


def hash_datapackage(datapackage):
    '''Returns a hash of the canonized version of the given datapackage
    (metadata).
    '''
    canonized = canonized_datapackage(datapackage)
    m = hashlib.md5(six.text_type(make_hashable(canonized)).encode('utf8'))
    return m.hexdigest()


def make_hashable(obj):
    if isinstance(obj, (tuple, list)):
        return tuple((make_hashable(e) for e in obj))
    if isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    return obj


def canonized_datapackage(datapackage):
    '''
    The given datapackage is 'canonized', so that an exsting one can be
    compared with a freshly generated one, to see if the zip needs
    regenerating.

    Datapackages resources have either:
    * local paths (downloaded into the package) OR
    * OR remote paths (URLs)
    To allow datapackages to be compared, the canonization converts local
    resources to remote ones.
    '''
    datapackage_ = copy.deepcopy(datapackage)
    # convert resources to remote paths
    # i.e.
    #
    #   "path": "annual-.csv", "sources": [
    #     {
    #       "path": "https://example.com/file.csv",
    #       "title": "annual.csv"
    #     }
    #   ],
    #
    # ->
    #
    #   "path": "https://example.com/file.csv",
    for res in datapackage_.get('resources', []):
        try:
            remote_path = res['sources'][0]['path']
        except KeyError:
            continue
        res['path'] = remote_path
        del res['sources']
    return datapackage_


def generate_datapackage_json(package_id, user=None):
    '''Generates the datapackage - metadata that would be saved as
    datapackage.json.

    :param package_id: The ID of the package/dataset
    :param user: The username of the authenticated user who initiated the task.
        This is used to ensure the background task has the same permissions as the user.
    '''
    log.info('Generating datapackage.json for package ID: {}'.format(package_id))
    try:
        context = {'model': model, 'session': model.Session}
        if user:
            context['user'] = user
        else:
            # When no user is provided (background job), use ignore_auth to bypass authorization
            context['ignore_auth'] = True
        dataset = get_action('package_show')(
            context, {'id': package_id})
        log.info('Retrieved dataset: {} (name: {})'.format(package_id, dataset.get('name', 'unknown')))

        # filter out resources that are not suitable for inclusion in the data
        # package
        # Use the authenticated user for the LocalCKAN instance
        local_ckan = ckanapi.LocalCKAN(username=user) if user else ckanapi.LocalCKAN()
        log.info('Filtering resources for inclusion in datapackage')
        dataset, resources_to_include, existing_zip_resource = \
            remove_resources_that_should_not_be_included_in_the_datapackage(
                dataset)
        log.info('Found {} resources to include in datapackage'.format(len(resources_to_include)))

        if existing_zip_resource:
            log.info('Found existing zip resource with ID: {}'.format(existing_zip_resource.get('id', 'unknown')))
        else:
            log.info('No existing zip resource found')

        # get the datapackage (metadata)
        log.info('Converting dataset to datapackage format')
        datapackage = ckanapi.datapackage.dataset_to_datapackage(dataset)
        log.info('Successfully converted dataset to datapackage format')

        # populate datapackage with the schema from the Datastore data
        # dictionary
        log.info('Populating datapackage with schema from Datastore')
        ckan_and_datapackage_resources = zip(resources_to_include,
                                            datapackage.get('resources', []))
        ckan_and_datapackage_resources = list(ckan_and_datapackage_resources)  # Convert to list for reuse

        for i, (res, datapackage_res) in enumerate(ckan_and_datapackage_resources):
            log.info('Processing resource {}/{}: {}'.format(i+1, len(ckan_and_datapackage_resources), res.get('name', 'unnamed')))
            try:
                ckanapi.datapackage.populate_datastore_res_fields(
                    ckan=local_ckan, res=res)
                ckanapi.datapackage.populate_schema_from_datastore(
                    cres=res, dres=datapackage_res)
                log.info('Successfully populated schema for resource: {}'.format(res.get('name', 'unnamed')))
            except Exception as e:
                log.warning('Error populating schema for resource {}: {}'.format(res.get('name', 'unnamed'), str(e)))
                # Continue with other resources even if one fails

        # add in any other dataset fields, if configured
        fields_to_include = config.get(
            u'ckanext.downloadall.dataset_fields_to_add_to_datapackage',
            u'').split()
        if fields_to_include:
            log.info('Adding additional fields to datapackage: {}'.format(', '.join(fields_to_include)))
            for key in fields_to_include:
                datapackage[key] = dataset.get(key)

        log.info('Datapackage generation complete')
        return (datapackage, ckan_and_datapackage_resources,
                existing_zip_resource)
    except Exception as e:
        log.error('Error generating datapackage for package {}: {}'.format(package_id, str(e)))
        import traceback
        log.error(traceback.format_exc())
        raise


def get_original_filename(resource):
    """
    Extract the original filename from the resource URL or name.

    :param resource: The resource dictionary
    :returns: The original filename
    """
    # Try to extract filename from URL
    url = resource.get('url', '')
    if url:
        # Remove query parameters
        url_path = url.split('?')[0]
        # Get the last part of the path
        filename = url_path.split('/')[-1]
        if filename:
            return filename

    # If URL doesn't provide a filename, use the resource name
    name = resource.get('name', '')
    if name:
        # If name has an extension, use it as is
        if '.' in name:
            return name
        # Otherwise, add an extension based on format
        format = resource.get('format', '')
        if format:
            # Convert format to a simple extension
            if '/' in format:
                format = format.split('/')[-1]
            return f"{name}.{format}"

    # Fallback to datapackage resource filename
    return None

def write_zip(fp, datapackage, ckan_and_datapackage_resources, user=None):
    '''
    Downloads resources and writes the zip file.

    :param fp: Open file that the zip can be written to
    :param datapackage: The datapackage metadata
    :param ckan_and_datapackage_resources: List of (ckan_resource, datapackage_resource) tuples
    :param user: The username of the authenticated user who initiated the task
    '''
    with zipfile.ZipFile(fp, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) \
            as zipf:
        i = 0
        successful_downloads = 0
        total_resources = len(ckan_and_datapackage_resources)

        log.info('Starting to download {} resources'.format(total_resources))

        for res, dres in ckan_and_datapackage_resources:
            i += 1

            log.debug('Downloading resource {}/{}: {}'
                      .format(i, total_resources,
                              res['url']))

            # Try to get the original filename first
            original_filename = get_original_filename(res)

            # Fallback to datapackage resource filename if original not found
            filename = original_filename or ckanapi.datapackage.resource_filename(dres)

            log.info('Using filename for resource: {}'.format(filename))

            try:
                log.info('Attempting to download resource: {} (ID: {})'.format(res.get('name', 'unnamed'), res.get('id', 'unknown')))
                download_resource_into_zip(res['url'], filename, zipf, user=user, resource=res)
                log.info('Successfully downloaded and added resource to zip: {} (ID: {})'.format(res.get('name', 'unnamed'), res.get('id', 'unknown')))
                successful_downloads += 1
                save_local_path_in_datapackage_resource(dres, res, filename)
            except DownloadError as e:
                # The dres['path'] is left as the url - i.e. an 'external
                # resource' of the data package.
                log.error('Failed to download resource: {} (ID: {}). Error: {}'.format(res.get('name', 'unnamed'), res.get('id', 'unknown'), str(e)))

            # TODO optimize using the file_hash

        log.info('Successfully downloaded {} out of {} resources'.format(successful_downloads, total_resources))

        # Add the datapackage.json
        write_datapackage_json(datapackage, zipf)

        # If no resources were successfully downloaded, log a warning
        if successful_downloads == 0 and total_resources > 0:
            log.warning('No resources were successfully downloaded for this dataset. The zip file will only contain the datapackage.json.')

    statinfo = os.stat(fp.name)
    filesize = statinfo.st_size

    log.info('Zip created: {} {} bytes'.format(fp.name, filesize))

    return filesize


def save_local_path_in_datapackage_resource(datapackage_resource, res,
                                            filename):
    # save path in datapackage.json - i.e. now pointing at the file
    # bundled in the data package zip
    title = datapackage_resource.get('title') or res.get('title') \
        or res.get('name', '')
    datapackage_resource['sources'] = [
        {'title': title, 'path': datapackage_resource['path']}]
    datapackage_resource['path'] = filename


def download_resource_into_zip(url, filename, zipf, user=None, resource=None):
    """
    Download a resource and add it to the zip file.

    If the resource is a local upload (url_type='upload'), access it directly from the filesystem.
    If the resource is stored in cloud storage, use the appropriate cloud storage class to get the URL.
    Otherwise, download it via HTTP.

    :param url: The URL of the resource
    :param filename: The filename to use in the zip file
    :param zipf: The zip file object
    :param user: The username of the authenticated user who initiated the task
    :param resource: The resource dictionary (optional)
    """
    log.info('Starting download of resource: {} to {}'.format(url, filename))

    # Ensure filename doesn't contain characters that could cause issues
    safe_filename = filename.replace('/', '_').replace('\\', '_')
    if safe_filename != filename:
        log.info('Sanitized filename from {} to {}'.format(filename, safe_filename))
        filename = safe_filename

    # Check if this is a local resource (url_type='upload')
    if resource and resource.get('url_type') == 'upload':
        log.info('Resource is an upload, checking if it is local or in cloud storage')

        # First, check if the cloudstorage extension is enabled
        # First, check if the cloudstorage extension is enabled
        try:
            import ckan.plugins as p
            plugins_config = p.toolkit.config.get('ckan.plugins', '')
            if isinstance(plugins_config, list):
                cloudstorage_enabled = 'cloudstorage' in plugins_config
            elif isinstance(plugins_config, str):
                cloudstorage_enabled = 'cloudstorage' in plugins_config.split()
            else:
                cloudstorage_enabled = False  # Προεπιλογή σε false αν ο τύπος είναι μη αναμενόμενος
            if cloudstorage_enabled:
                log.info('Cloudstorage extension is enabled, checking if resource is in cloud storage')
                try:
                    from ckanext.cloudstorage.storage import ResourceCloudStorage

                    # Get the resource ID
                    resource_id = resource.get('id')
                    if not resource_id:
                        log.error('Resource ID not found for cloud resource')
                        raise DownloadError("Resource ID not found for cloud resource")

                    # Create a ResourceCloudStorage instance to get the URL
                    cloud_storage = ResourceCloudStorage(resource)
                    cloud_url = cloud_storage.get_path(resource_id)

                    if cloud_url:
                        log.info('Resource is in cloud storage, URL: {}'.format(cloud_url))
                        # Update the URL to use the cloud URL
                        url = cloud_url
                        # Skip the local file check and proceed to HTTP download
                        raise Exception("Resource is in cloud storage, proceeding to HTTP download")
                except Exception as e:
                    log.info('Error checking cloud storage or resource not in cloud: {}'.format(str(e)))
                    log.info('Falling back to local file check')
            else:
                log.info('Cloudstorage extension is not enabled, checking local file')
        except Exception as e:
            log.error('Error checking cloudstorage extension: {}'.format(str(e)))
            log.info('Falling back to local file check')

        # If we get here, try to access the file locally
        try:
            from ckan.lib.uploader import ResourceUpload

            # Get the resource ID
            resource_id = resource.get('id')
            if not resource_id:
                log.error('Resource ID not found for local resource')
                raise DownloadError("Resource ID not found for local resource")

            # Create a ResourceUpload instance to get the file path
            resource_upload = ResourceUpload(resource)
            filepath = resource_upload.get_path(resource_id)

            log.info('Local resource path: {}'.format(filepath))

            # Check if the file exists
            if not os.path.exists(filepath):
                log.error('Local resource file not found: {}'.format(filepath))
                raise DownloadError("Local resource file not found: {}".format(filepath))

            # Add the file to the zip
            hash_object = hashlib.md5()
            size = os.path.getsize(filepath)

            log.info('Adding local resource to zip: {}'.format(filepath))

            # Create ZipInfo object to preserve original file timestamp
            file_stat = os.stat(filepath)
            file_mtime = file_stat.st_mtime
            # Convert to tuple of (year, month, day, hour, minute, second)
            date_time = time.localtime(file_mtime)[:6]

            # Create ZipInfo with original timestamp
            info = zipfile.ZipInfo(filename)
            info.date_time = date_time
            info.compress_type = zipfile.ZIP_DEFLATED

            # Write file with preserved timestamp
            with open(filepath, 'rb') as f:
                zipf.writestr(info, f.read())

            log.info('Successfully added local resource to zip with original timestamp')

            # Calculate hash
            with open(filepath, 'rb') as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    hash_object.update(chunk)

            file_hash = hash_object.hexdigest()
            log.info('Added local resource {}, size: {}, hash: {}'.format(
                filepath, format_bytes(size), file_hash))


        except Exception as e:
            log.error('Error accessing local resource: {}'.format(str(e)))
            import traceback
            log.error(traceback.format_exc())
            log.info('Falling back to HTTP download')
            # Fall back to HTTP download

    # If not a local resource or if accessing it directly failed, download via HTTP
    try:
        log.info('Making HTTP request to: {}'.format(url))
        # Create a session with the user's API key if available
        session = requests.Session()
        if user:
            # Get the user's API key
            from ckan import model
            user_obj = model.User.get(user)
            if user_obj and user_obj.apikey:
                session.headers.update({'Authorization': user_obj.apikey})
                log.info('Using API key for user: {}'.format(user))

        # Make the request with the authenticated session
        r = session.get(url, stream=True)
        r.raise_for_status()
        log.info('HTTP request successful, status code: {}'.format(r.status_code))
    except requests.ConnectionError:
        error_msg = 'URL {url} refused connection. The resource will not be downloaded'.format(url=url)
        log.error(error_msg)
        raise DownloadError(error_msg)
    except requests.exceptions.HTTPError as e:
        error_msg = 'URL {url} status error: {status}. The resource will not be downloaded'.format(url=url, status=e.response.status_code)
        log.error(error_msg)
        raise DownloadError(error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = 'URL {url} download request exception: {error}'.format(url=url, error=str(e))
        log.error(error_msg)
        raise DownloadError(error_msg)
    except Exception as e:
        error_msg = 'URL {url} download exception: {error}'.format(url=url, error=str(e))
        log.error(error_msg)
        import traceback
        log.error(traceback.format_exc())
        raise DownloadError(error_msg)

    hash_object = hashlib.md5()
    size = 0
    try:
        log.info('Attempting to write resource to zip file using Python 3 method')

        # Try to get Last-Modified header from response
        last_modified = None
        if 'Last-Modified' in r.headers:
            try:
                # Parse the Last-Modified header
                last_modified_str = r.headers['Last-Modified']
                log.info('Found Last-Modified header: {}'.format(last_modified_str))
                # Parse RFC 7232 date format
                from email.utils import parsedate_to_datetime
                last_modified = parsedate_to_datetime(last_modified_str).timestamp()
                log.info('Parsed Last-Modified timestamp: {}'.format(last_modified))
            except Exception as e:
                log.warning('Failed to parse Last-Modified header: {}'.format(str(e)))
                last_modified = None

        # If we couldn't get Last-Modified, use current time
        if last_modified is None:
            last_modified = time.time()
            log.info('Using current time for file timestamp')

        # Create ZipInfo with timestamp
        date_time = time.localtime(last_modified)[:6]
        info = zipfile.ZipInfo(filename)
        info.date_time = date_time
        info.compress_type = zipfile.ZIP_DEFLATED

        # Download content to memory first
        content = b''
        for chunk in r.iter_content(chunk_size=16384):  # Increased chunk size to 16KB
            if chunk:  # Filter out keep-alive new chunks
                content += chunk
                hash_object.update(chunk)
                size += len(chunk)

        # Write content with preserved timestamp
        zipf.writestr(info, content)

        log.info('Successfully wrote resource to zip using Python 3 method with preserved timestamp')

    except Exception as e:
        error_msg = 'Error writing resource to zip: {}'.format(str(e))
        log.error(error_msg)
        import traceback
        log.error(traceback.format_exc())
        raise DownloadError(error_msg)

    file_hash = hash_object.hexdigest()
    log.info('Downloaded {}, hash: {}'
              .format(format_bytes(size), file_hash))


def write_datapackage_json(datapackage, zipf):
    log.info('Writing datapackage.json to zip file')
    try:
        with tempfile.NamedTemporaryFile() as json_file:
            log.info('Creating temporary file for datapackage.json')
            json_content = ckanapi.cli.utils.pretty_json(datapackage)
            log.info('Generated JSON content, size: {} bytes'.format(len(json_content)))
            json_file.write(json_content)
            json_file.flush()
            log.info('Writing datapackage.json from temporary file: {}'.format(json_file.name))

            # Use current time for datapackage.json since it's dynamically generated
            current_time = time.time()
            date_time = time.localtime(current_time)[:6]

            # Create ZipInfo with current timestamp
            info = zipfile.ZipInfo('datapackage.json')
            info.date_time = date_time
            info.compress_type = zipfile.ZIP_DEFLATED

            # Write file with current timestamp
            with open(json_file.name, 'rb') as f:
                zipf.writestr(info, f.read())

            log.info('Successfully added datapackage.json to zip file with current timestamp')
    except Exception as e:
        log.error('Error writing datapackage.json to zip: {}'.format(str(e)))
        import traceback
        log.error(traceback.format_exc())
        raise


def format_bytes(size_bytes):
    if size_bytes == 0:
        return "0 bytes"
    size_name = ("bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 1)
    return '{} {}'.format(s, size_name[i])


def remove_resources_that_should_not_be_included_in_the_datapackage(dataset):
    log.info('Filtering resources for dataset: {}'.format(dataset.get('name', 'unknown')))
    resource_formats_to_ignore = ['API', 'api']  # TODO make it configurable
    log.info('Resource formats to ignore: {}'.format(resource_formats_to_ignore))

    existing_zip_resource = None
    resources_to_include = []
    total_resources = len(dataset['resources'])
    log.info('Total resources in dataset: {}'.format(total_resources))

    for i, res in enumerate(dataset['resources']):
        resource_id = res.get('id', 'unknown')
        resource_name = res.get('name', 'unnamed')
        resource_format = res.get('format', 'unknown')

        log.info('Processing resource {}/{}: {} (ID: {}, Format: {})'.format(
            i + 1, total_resources, resource_name, resource_id, resource_format))

        if res.get('downloadall_metadata_modified'):
            # this is an existing zip of all the other resources
            log.info('Resource {}/{} skipped - is the zip itself (ID: {})'.format(
                i + 1, total_resources, resource_id))
            existing_zip_resource = res
            continue

        if resource_format in resource_formats_to_ignore:
            log.info('Resource {}/{} skipped - because it is format {} (ID: {})'.format(
                i + 1, total_resources, resource_format, resource_id))
            continue

        log.info('Resource {}/{} included: {} (ID: {})'.format(
            i + 1, total_resources, resource_name, resource_id))
        resources_to_include.append(res)

    log.info('Resources after filtering: {} of {} will be included'.format(
        len(resources_to_include), total_resources))

    if existing_zip_resource:
        log.info('Existing zip resource found: {} (ID: {})'.format(
            existing_zip_resource.get('name', 'unnamed'), 
            existing_zip_resource.get('id', 'unknown')))
    else:
        log.info('No existing zip resource found')

    dataset = dict(dataset, resources=resources_to_include)
    return dataset, resources_to_include, existing_zip_resource

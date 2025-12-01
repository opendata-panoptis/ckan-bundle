# -*- coding: utf-8 -*-

import os
import logging
import mimetypes
from six.moves.urllib.parse import urlparse, parse_qs


from ckan import plugins as p
from ckan.common import json
from ckan.lib.datapreview import on_same_domain
from ckan.plugins import toolkit

import ckanext.geoview.utils as utils

if toolkit.check_ckan_version("2.9"):
    from ckanext.geoview.plugin.flask_plugin import GeoViewMixin
else:
    from ckanext.geoview.plugin.pylons_plugin import GeoViewMixin

ignore_empty = toolkit.get_validator("ignore_empty")
boolean_validator = toolkit.get_validator("boolean_validator")

log = logging.getLogger(__name__)


class GeoViewBase(p.SingletonPlugin):
    """This base class is for view extensions. """

    p.implements(p.IResourceView, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)

    proxy_enabled = False
    same_domain = False

    def configure(self, config):
        basemapConfigFile = toolkit.config.get(
            "ckanext.geoview.basemaps", None
        )
        self.basemapsConfig = basemapConfigFile and utils.load_basemaps(
            basemapConfigFile
        )

    def update_config(self, config):
        toolkit.add_public_directory(config, "../public")
        toolkit.add_template_directory(config, "../templates")
        toolkit.add_resource("../public", "ckanext-geoview")

        self.proxy_enabled = "resource_proxy" in toolkit.config.get(
            "ckan.plugins", ""
        )


class OLGeoView(GeoViewMixin, GeoViewBase):

    p.implements(p.ITemplateHelpers)
    p.implements(p.IBlueprint)

    # IBlueprint
    def get_blueprint(self):
        from flask import Blueprint, Response
        import requests

        # Get the parent blueprints
        parent_blueprints = super(OLGeoView, self).get_blueprint()

        # Create our own blueprint for KML handling
        kml_bp = Blueprint('geo_view_kml', __name__)

        # Add route for KML proxy
        kml_bp.add_url_rule(
            '/kml_proxy/<resource_id>/<filename>',
            view_func=self.kml_proxy,
            methods=['GET', 'OPTIONS']
        )

        # Add handler for CORS preflight OPTIONS requests
        @kml_bp.route('/kml_proxy/<resource_id>/<filename>', methods=['OPTIONS'])
        def handle_options_kml(resource_id, filename):
            # Handle CORS preflight request for the KML proxy
            response = Response('')
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'  # 24 hours
            })
            return response

        # Return a list containing both the parent blueprints and our new blueprint
        if isinstance(parent_blueprints, list):
            return parent_blueprints + [kml_bp]
        else:
            return [parent_blueprints, kml_bp]

    def kml_proxy(self, resource_id, filename):
        """
        This is our custom proxy for KML files. It fetches KML data directly from Azure
        and serves it to the user, avoiding CORS issues.
        """
        from flask import Response
        from ckan.plugins import toolkit
        import ckan.lib.uploader as uploader

        try:
            # Check permissions using the current user's context
            context = {'user': toolkit.c.user}
            resource = toolkit.get_action('resource_show')(context, {'id': resource_id})

            if not resource.get('url_type') == 'upload':
                return toolkit.abort(404, 'Resource not an upload')

            # Get the uploader for this resource
            resource_uploader = uploader.get_resource_uploader(resource)

            # Get the path to the file in storage
            # Handle different uploader interfaces: some take (id, filename), others just (id)
            try:
                # Try the 3-argument version (AzureResourceUploader)
                blob_path = resource_uploader.get_path(resource_id, filename)
            except TypeError:
                # Fall back to 2-argument version (ResourceCloudStorage, standard CKAN)
                # For these uploaders, we need to construct the path manually
                blob_path = resource_uploader.get_path(resource_id)
                if hasattr(resource_uploader, 'path_from_filename'):
                    # ResourceCloudStorage has this method
                    blob_path = resource_uploader.path_from_filename(resource_id, filename)
                else:
                    # For standard CKAN uploader, append filename manually
                    import os
                    blob_path = os.path.join(blob_path, filename)

            log.debug("KML proxy fetching data directly from storage: %s", blob_path)

            # Get the Azure Blob Storage configuration
            from ckanext.azurefilestore.uploader import BaseAzureUploader
            azure_uploader = BaseAzureUploader()

            # Get the blob service client
            blob_service_client = azure_uploader.get_blob_service_client()
            container_name = toolkit.config.get('ckanext.azurefilestore.container_name')

            # Get the blob client for this file
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

            # Download the blob content
            downloader = blob_client.download_blob()
            content_bytes = downloader.readall()

            # KML is XML-based. No JSON parsing. Just return the content.
            # Use the correct MIME type for KML.
            response = Response(
                content_bytes,
                mimetype='application/vnd.google-earth.kml+xml'
            )
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'  # 24 hours
            })
            return response

        except Exception as e:
            log.error("Error in kml_proxy: %s", str(e))
            response = Response(
                f"Error fetching KML data: {str(e)}",
                status=500,
                mimetype='text/plain'
            )
            # Add CORS headers even to error responses
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'  # 24 hours
            })
            return response

    GEOVIEW_FORMATS = [
        "kml",
        "geojson",
        "gml",
        "wms",
        "wfs",
        "esrigeojson",
        "gft",
        "arcgis_rest",
        "wmts",
        "esri rest",
    ]

    # ITemplateHelpers

    def get_helpers(self):
        return {
            "get_common_map_config_geoviews": utils.get_common_map_config,
            "get_openlayers_viewer_config": utils.get_openlayers_viewer_config,
        }

    # IResourceView

    def info(self):
        return {
            "name": "geo_view",
            "title": "Map viewer (OpenLayers)",
            "icon": "globe",
            "iframed": True,
            "default_title": toolkit._("Map viewer"),
            "schema": {
                "feature_hoveron": [ignore_empty, boolean_validator],
                "feature_style": [ignore_empty],
            },
        }

    def can_view(self, data_dict):
        format_lower = data_dict["resource"].get("format", "").lower()
        same_domain = on_same_domain(data_dict)

        # Guess from file extension
        if not format_lower and data_dict["resource"].get("url"):
            format_lower = self._guess_format_from_extension(
                data_dict["resource"]["url"]
            )

        if not format_lower:
            return False

        view_formats = toolkit.config.get(
            "ckanext.geoview.ol_viewer.formats", ""
        )
        if view_formats:
            view_formats = view_formats.split(" ")
        else:
            view_formats = self.GEOVIEW_FORMATS

        correct_format = format_lower in view_formats
        can_preview_from_domain = self.proxy_enabled or same_domain

        return correct_format and can_preview_from_domain

    def view_template(self, context, data_dict):
        return "dataviewer/openlayers.html"

    def form_template(self, context, data_dict):
        return "dataviewer/openlayers_form.html"

    def _guess_format_from_extension(self, url):
        try:
            parsed_url = urlparse(url)
            format_lower = (
                os.path.splitext(parsed_url.path)[1][1:]
                .encode("ascii", "ignore")
                .lower()
            )
        except ValueError as e:
            log.error("Invalid URL: {0}, {1}".format(url, e))
            format_lower = ""

        return format_lower

    def setup_template_variables(self, context, data_dict):
        import ckanext.resourceproxy.plugin as proxy
        from ckan.lib.helpers import url_for

        same_domain = on_same_domain(data_dict)

        if not data_dict["resource"].get("format"):
            data_dict["resource"][
                "format"
            ] = self._guess_format_from_extension(data_dict["resource"]["url"])

        resource = data_dict["resource"]
        format_lower = (resource.get("format") or "").lower()

        # Use proxy only for cross-origin resources when proxy is enabled
        proxy_url = None
        proxy_service_url = None
        
        if self.proxy_enabled and not same_domain:
            # For KML files, use our custom KML proxy instead of generic resource proxy
            if format_lower == 'kml' and resource.get('url_type') == 'upload':
                # Extract filename from the resource URL
                filename = os.path.basename(urlparse(resource['url']).path)
                proxy_url = url_for(
                    'geo_view_kml.kml_proxy',
                    resource_id=resource['id'],
                    filename=filename
                )
                log.debug("Using custom KML proxy URL: %s", proxy_url)
            else:
                proxy_url = proxy.get_proxified_resource_url(data_dict)             # .../proxy?url=...
                proxy_service_url = utils.get_proxified_service_url(data_dict)      # .../resource_proxy
        # For same-origin, leave both as None so template omits data attrs and client uses raw URL

        gapi_key = toolkit.config.get("ckanext.geoview.gapi_key")
        return {
            "resource_view_json": "resource_view" in data_dict
            and json.dumps(data_dict["resource_view"]),
            "proxy_service_url": proxy_service_url,
            "proxy_url": proxy_url,
            "gapi_key": gapi_key,
            "basemapsConfig": self.basemapsConfig,
        }


class GeoJSONView(GeoViewBase):
    p.implements(p.ITemplateHelpers, inherit=True)
    p.implements(p.IBlueprint)
    p.implements(p.IResourceController, inherit=True)

    GeoJSON = ["geojson"]
    
    # IBlueprint
    def get_blueprint(self):
        from flask import Blueprint, Response, request
        import requests
        
        bp = Blueprint('geojson_view', __name__)
        
        # Route for /geoview/<resource_id>/<filename> - direct SAS redirect
        bp.add_url_rule(
            '/geoview/<resource_id>/<filename>',
            view_func=self.geoview_sas_redirect,
            methods=['GET']
        )
        
        # Route for /geoview_proxy/<resource_id>/<filename> - proxy that fetches data
        bp.add_url_rule(
            '/geoview_proxy/<resource_id>/<filename>',
            view_func=self.geoview_proxy,
            methods=['GET', 'OPTIONS']  # Add OPTIONS method for CORS preflight
        )

        # Route for /geoview_file_proxy/<resource_id>/<filename> - generic file proxy (eg KML/KMZ)
        bp.add_url_rule(
            '/geoview_file_proxy/<resource_id>/<filename>',
            view_func=self.geoview_file_proxy,
            methods=['GET', 'OPTIONS']
        )
        
        # Add a handler for CORS preflight OPTIONS requests
        @bp.route('/geoview_proxy/<resource_id>/<filename>', methods=['OPTIONS'])
        def handle_options(resource_id, filename):
            response = Response('')
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'  # 24 hours
            })
            return response

        # OPTIONS for file proxy as well
        @bp.route('/geoview_file_proxy/<resource_id>/<filename>', methods=['OPTIONS'])
        def handle_file_options(resource_id, filename):
            response = Response('')
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'
            })
            return response
        
        return bp
        
    def geoview_sas_redirect(self, resource_id, filename):
        import os
        from ckanext.azurefilestore.uploader import BaseAzureUploader
        from ckanext.azurefilestore.controller import redirect
        
        # Create the same blob_path as the archiver
        blob_path = os.path.join('cached', resource_id[:2], resource_id, filename)
        uploader = BaseAzureUploader()
        sas_url = uploader.generate_sas_url(blob_path)
        log.debug("GeoView redirect to SAS URL: %s", sas_url)
        return redirect(sas_url)
        
    def geoview_proxy(self, resource_id, filename):
        """
        Proxy endpoint that fetches GeoJSON data from Azure Blob Storage
        and returns it to the browser, avoiding CORS issues.
        """
        import os
        import json
        import re
        from flask import Response
        from ckan.plugins import toolkit
        import ckan.lib.uploader as uploader
        
        try:
            # Get the resource information
            context = {'user': toolkit.c.user}
            resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
            
            # Check if the resource is an upload
            if not resource.get('url_type') == 'upload':
                return toolkit.abort(404, 'Resource not an upload')
            
            # Get the uploader for this resource
            resource_uploader = uploader.get_resource_uploader(resource)
            
            # Get the path to the file in storage
            # Handle different uploader interfaces: some take (id, filename), others just (id)
            try:
                # Try the 3-argument version (AzureResourceUploader)
                blob_path = resource_uploader.get_path(resource_id, filename)
            except TypeError:
                # Fall back to 2-argument version (ResourceCloudStorage, standard CKAN)
                # For these uploaders, we need to construct the path manually
                blob_path = resource_uploader.get_path(resource_id)
                if hasattr(resource_uploader, 'path_from_filename'):
                    # ResourceCloudStorage has this method
                    blob_path = resource_uploader.path_from_filename(resource_id, filename)
                else:
                    # For standard CKAN uploader, append filename manually
                    import os
                    blob_path = os.path.join(blob_path, filename)
            
            log.debug("GeoView proxy fetching data directly from storage: %s", blob_path)
            
            # Get the Azure Blob Storage configuration
            from ckanext.azurefilestore.uploader import BaseAzureUploader
            azure_uploader = BaseAzureUploader()
            
            # Get the blob service client
            blob_service_client = azure_uploader.get_blob_service_client()
            container_name = toolkit.config.get('ckanext.azurefilestore.container_name')
            
            # Get the blob client for this file
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
            
            # Download the blob content
            downloader = blob_client.download_blob()
            content_bytes = downloader.readall()
            
            # Parse the response content as JSON to validate it
            try:
                # Get the response content as text
                content_text = content_bytes.decode('utf-8-sig')  # Use utf-8-sig to remove BOM if present
                
                # Remove any comments or non-JSON content
                # This regex pattern matches JavaScript-style comments (both // and /* */)
                content_text = re.sub(r'//.*?$|/\*.*?\*/', '', content_text, flags=re.MULTILINE|re.DOTALL)
                
                # Parse the content as JSON to validate it
                json_data = json.loads(content_text)
                
                # Return the validated JSON data with full CORS headers
                response = Response(
                    json.dumps(json_data),
                    mimetype='application/json'
                )
                # Add all necessary CORS headers
                response.headers.update({
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Max-Age': '86400'  # 24 hours
                })
                return response
            except json.JSONDecodeError as json_err:
                log.error("Error parsing GeoJSON data: %s", str(json_err))
                response = Response(
                    f"Error parsing GeoJSON data: The file is not valid JSON. Please check the file format.",
                    status=400,
                    mimetype='text/plain'
                )
                # Add CORS headers even to error responses
                response.headers.update({
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Max-Age': '86400'  # 24 hours
                })
                return response
        except Exception as e:
            log.error("Error in geoview_proxy: %s", str(e))
            response = Response(
                f"Error fetching GeoJSON data: {str(e)}",
                status=500,
                mimetype='text/plain'
            )
            # Add CORS headers even to general error responses
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'  # 24 hours
            })
            return response

    def geoview_file_proxy(self, resource_id, filename):
        """
        Generic file proxy (eg, KML/KMZ) that fetches bytes from Azure Blob Storage
        and returns them directly to the browser with appropriate CORS headers.
        """
        from flask import Response
        from ckan.plugins import toolkit
        import ckan.lib.uploader as uploader
        import os

        try:
            # Get the resource information
            context = {'user': toolkit.c.user}
            resource = toolkit.get_action('resource_show')(context, {'id': resource_id})

            # Ensure it's an uploaded resource stored in Azure
            if not resource.get('url_type') == 'upload':
                return toolkit.abort(404, 'Resource not an upload')

            # Resolve blob path via the resource-specific uploader
            resource_uploader = uploader.get_resource_uploader(resource)
            
            # Handle different uploader interfaces: some take (id, filename), others just (id)
            try:
                # Try the 3-argument version (AzureResourceUploader)
                blob_path = resource_uploader.get_path(resource_id, filename)
            except TypeError:
                # Fall back to 2-argument version (ResourceCloudStorage, standard CKAN)
                # For these uploaders, we need to construct the path manually
                blob_path = resource_uploader.get_path(resource_id)
                if hasattr(resource_uploader, 'path_from_filename'):
                    # ResourceCloudStorage has this method
                    blob_path = resource_uploader.path_from_filename(resource_id, filename)
                else:
                    # For standard CKAN uploader, append filename manually
                    import os
                    blob_path = os.path.join(blob_path, filename)

            log.debug("GeoView file proxy fetching bytes from storage: %s", blob_path)

            # Azure clients
            from ckanext.azurefilestore.uploader import BaseAzureUploader
            azure_uploader = BaseAzureUploader()
            blob_service_client = azure_uploader.get_blob_service_client()
            container_name = toolkit.config.get('ckanext.azurefilestore.container_name')
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

            # Download file bytes
            downloader = blob_client.download_blob()
            content_bytes = downloader.readall()

            # Determine content type
            ext = os.path.splitext(filename.lower())[1]
            if ext == '.kml':
                content_type = 'application/vnd.google-earth.kml+xml'
            elif ext == '.kmz':
                content_type = 'application/vnd.google-earth.kmz'
            else:
                content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'

            # Build response
            response = Response(content_bytes, mimetype=content_type)
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'
            })
            return response
        except Exception as e:
            log.error("Error in geoview_file_proxy: %s", str(e))
            response = Response(
                f"Error fetching file data: {str(e)}",
                status=500,
                mimetype='text/plain'
            )
            response.headers.update({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'
            })
            return response

    def update_config(self, config):

        super(GeoJSONView, self).update_config(config)

        mimetypes.add_type("application/geo+json", ".geojson")

    # IResourceView
    def info(self):
        return {
            "name": "geojson_view",
            "title": "GeoJSON",
            "icon": "map-marker",
            "iframed": True,
            "default_title": toolkit._("GeoJSON"),
        }

    def can_view(self, data_dict):
        resource = data_dict["resource"]

        format_lower = resource.get("format", "").lower()
        original_format = format_lower

        same_domain = on_same_domain(data_dict)

        # Always check file extension for format detection
        # This handles cases where CKAN incorrectly identifies .geojson files
        if resource.get("url"):
            guessed_format = self._guess_format_from_extension(resource["url"])
            log.debug("GeoJSONView format detection: original='%s', guessed='%s', url='%s'", 
                     original_format, guessed_format, resource.get("url"))
            
            # If we detect a .geojson extension, always use it regardless of the current format
            if guessed_format == "geojson":
                format_lower = "geojson"
                log.debug("GeoJSONView: Corrected format from '%s' to 'geojson' based on file extension", original_format)

        # If still not identified as geojson, use heuristics to detect GeoJSON served as JSON
        if format_lower != "geojson" and self._looks_like_geojson(resource):
            log.debug("GeoJSONView: Resource looks like GeoJSON based on heuristics; treating as 'geojson'")
            format_lower = "geojson"

        if format_lower in self.GeoJSON:
            return same_domain or self.proxy_enabled
        return False

    def _guess_format_from_extension(self, url):
        try:
            parsed_url = urlparse(url)
            format_lower = (
                os.path.splitext(parsed_url.path)[1][1:]
                .encode("ascii", "ignore")
                .decode("ascii")
                .lower()
            )
        except ValueError as e:
            log.error("Invalid URL: {0}, {1}".format(url, e))
            format_lower = ""

        return format_lower

    def _looks_like_geojson(self, resource):
        """
        Lightweight heuristics to detect GeoJSON even if the extension is .json
        or the format was set to 'json'. This avoids fetching content.
        """
        try:
            url = (resource.get('url') or '').strip()
            name = (resource.get('name') or '').strip().lower()
            # Check known mimetype fields
            mt = (
                (resource.get('mimetype') or '')
                or (resource.get('mimetype_inner') or '')
                or (resource.get('media_type') or '')
            ).lower()
            if 'geo+json' in mt:
                return True

            if url:
                parsed = urlparse(url)
                path_lower = (parsed.path or '').lower()
                if 'geojson' in path_lower:
                    return True
                # Look into common query parameters
                q = parse_qs(parsed.query or '')
                for key in ('format', 'f', 'type', 'outputformat', 'outputFormat'):
                    values = q.get(key) or q.get(key.lower())
                    if values and any(isinstance(v, str) and 'geojson' in v.lower() for v in values):
                        return True

            if 'geojson' in name:
                return True
        except Exception as e:
            log.debug('GeoJSONView._looks_like_geojson error: %s', e)
        return False

    def view_template(self, context, data_dict):
        return "dataviewer/geojson.html"

    def setup_template_variables(self, context, data_dict):
        from ckan.lib.helpers import url_for
        import os
        
        resource = data_dict['resource']
        original_url = resource.get('url')
        
        if resource.get('url_type') == 'upload':
            # For uploads we proxy via Azure to avoid exposing storage URLs
            filename = os.path.basename(urlparse(original_url or '').path)
            if not filename:
                filename = f"{resource['id']}.geojson"

            proxy_url = url_for(
                'geojson_view.geoview_proxy',
                resource_id=resource['id'],
                filename=filename
            )
            data_dict['resource']['url'] = proxy_url
            log.debug("Using GeoJSON upload proxy URL: %s", proxy_url)
            return

        # For linked resources prefer CKAN's resource proxy if available,
        # so that we can fetch remote GeoJSON without CORS issues.
        if self.proxy_enabled:
            package_id = (
                resource.get('package_id')
                or data_dict.get('package', {}).get('id')
                or data_dict.get('package_id')
            )

            if package_id:
                proxy_url = url_for(
                    'resource_proxy.proxy_view',
                    id=package_id,
                    resource_id=resource['id']
                )
                data_dict['resource']['url'] = proxy_url
                log.debug(
                    "Using CKAN resource proxy URL for GeoJSON: %s (original: %s)",
                    proxy_url,
                    original_url,
                )
                return

        # Fall back to the original URL if no proxy applies
        data_dict['resource']['url'] = original_url
        log.debug("Using original GeoJSON URL (no proxy applied): %s", original_url)

    # ITemplateHelpers

    def get_helpers(self):
        return {
            "get_common_map_config_geojson": utils.get_common_map_config,
            "geojson_get_max_file_size": utils.get_max_file_size,
            "get_geoview_sas_url": self.get_geoview_sas_url,
        }
        
    def get_geoview_sas_url(self, resource_id, filename):
        """
        Generate a URL for the geoview SAS redirect route
        """
        from ckan.lib.helpers import url_for
        return url_for(
            'geojson_view.geoview_sas_redirect',
            resource_id=resource_id,
            filename=filename
        )

    # IResourceController methods
    
    def before_resource_create(self, context, resource):
        """
        Intercept resource creation to correct format for GeoJSON files.
        
        This ensures that .geojson files are properly categorized as 'geojson' 
        format rather than being incorrectly stored as 'json' in CKAN.
        """
        self._correct_geojson_format(resource)
    
    def before_resource_update(self, context, current, resource):
        """
        Intercept resource updates to correct format for GeoJSON files.
        
        This ensures that .geojson files are properly categorized as 'geojson' 
        format rather than being incorrectly stored as 'json' in CKAN.
        """
        self._correct_geojson_format(resource)
    
    def _correct_geojson_format(self, resource):
        """
        Helper method to detect and correct GeoJSON format based on file extension
        or other available hints at creation time (eg upload filename, name, mimetype).
        
        :param resource: The resource dictionary being created/updated
        """
        current_format = (resource.get('format') or '').lower()
        url = (resource.get('url') or '').strip()
        guessed_format = ''

        # 1) Try URL first (works for links and for uploads after CKAN sets the URL)
        if url:
            guessed_format = self._guess_format_from_extension(url)

        # 2) If no URL or not geojson, try to infer from upload filename
        if guessed_format != 'geojson':
            upload = resource.get('upload')
            candidate_names = []
            if upload is not None:
                # werkzeug.datastructures.FileStorage has .filename
                filename = getattr(upload, 'filename', None)
                if not filename and isinstance(upload, dict):
                    filename = upload.get('filename')
                if not filename and isinstance(upload, str):
                    filename = upload
                if filename:
                    candidate_names.append(filename)

            # 3) Also check the resource name as a fallback
            name_val = resource.get('name')
            if isinstance(name_val, str) and name_val:
                candidate_names.append(name_val)

            # Check all candidates for a .geojson extension
            for cand in candidate_names:
                try:
                    ext = os.path.splitext(cand)[1][1:].lower()
                    if ext == 'geojson':
                        guessed_format = 'geojson'
                        url_or_name = url or cand
                        log.debug("GeoJSONView: Detected .geojson from candidate '%s'", cand)
                        break
                except Exception:
                    pass

        # 4) If still not identified as geojson, apply lightweight heuristics
        if guessed_format != 'geojson' and self._looks_like_geojson(resource):
            guessed_format = 'geojson'

        # 5) Apply correction if needed
        if guessed_format == 'geojson':
            log.debug("GeoJSONView: Correcting resource format from '%s' to 'geojson' (url/name/upload checked)", current_format)
            resource['format'] = 'geojson'



class WMTSView(GeoViewBase):
    p.implements(p.ITemplateHelpers, inherit=True)

    WMTS = ["wmts"]

    # IResourceView
    def info(self):
        return {
            "name": "wmts_view",
            "title": "wmts",
            "icon": "map-marker",
            "iframed": True,
            "default_title": toolkit._("WMTS"),
        }

    def can_view(self, data_dict):
        resource = data_dict["resource"]
        format_lower = resource.get("format", "").lower()
        same_domain = on_same_domain(data_dict)

        if format_lower in self.WMTS:
            return same_domain or self.proxy_enabled
        return False

    def view_template(self, context, data_dict):
        return "dataviewer/wmts.html"

    def setup_template_variables(self, context, data_dict):
        import ckanext.resourceproxy.plugin as proxy

        self.same_domain = data_dict["resource"].get("on_same_domain")
        if self.proxy_enabled and not self.same_domain:
            data_dict["resource"]["original_url"] = data_dict["resource"].get(
                "url"
            )
            data_dict["resource"]["url"] = proxy.get_proxified_resource_url(
                data_dict
            )

    # ITemplateHelpers

    def get_helpers(self):
        return {
            "get_common_map_config_wmts": utils.get_common_map_config,
        }


class SHPView(GeoViewBase):
    p.implements(p.ITemplateHelpers, inherit=True)

    SHP = ["shp", "shapefile"]

    # IResourceView
    def info(self):
        return {
            "name": "shp_view",
            "title": "Shapefile",
            "icon": "map-marker",
            "iframed": True,
            "default_title": p.toolkit._("Shapefile"),
        }

    def can_view(self, data_dict):
        resource = data_dict["resource"]
        format_lower = resource.get("format", "").lower()
        name_lower = resource.get("name", "").lower()
        same_domain = on_same_domain(data_dict)

        if format_lower in self.SHP or any([shp in name_lower for shp in self.SHP]):
            return same_domain or self.proxy_enabled
        return False

    def view_template(self, context, data_dict):
        return "dataviewer/shp.html"

    def setup_template_variables(self, context, data_dict):
        import ckanext.resourceproxy.plugin as proxy

        self.same_domain = data_dict["resource"].get("on_same_domain")
        if self.proxy_enabled and not self.same_domain:
            data_dict["resource"]["original_url"] = data_dict["resource"].get(
                "url"
            )
            data_dict["resource"]["url"] = proxy.get_proxified_resource_url(
                data_dict
            )

    # ITemplateHelpers

    def get_helpers(self):
        return {
            "get_common_map_config_shp": utils.get_common_map_config,
            "get_shapefile_viewer_config": utils.get_shapefile_viewer_config,
        }


class SDGMapView(GeoViewBase):
    """Custom view for integrating Greek SDGMap service"""
    p.implements(p.ITemplateHelpers, inherit=True)

    # IResourceView
    def info(self):
        return {
            "name": "sdgmap_view",
            "title": "Ενιαίος Ψηφιακός Χάρτης",
            "icon": "map-marker",
            "iframed": True,
            "default_title": p.toolkit._("Ενιαίος Ψηφιακός Χάρτης"),
        }

    def can_view(self, data_dict):
        # This view can be used for any geographic resource
        # but will be manually selected by users when they want SDGMap integration
        resource = data_dict["resource"]
        format_lower = resource.get("format", "").lower()
        
        # Accept common geographic formats that could benefit from SDGMap integration
        geo_formats = ["kml", "kmz", "geojson", "shp", "shapefile", "gml", "wms", "wmts"]
        
        if format_lower in geo_formats:
            return True
        
        # Also check if the resource name contains geographic indicators
        name_lower = (resource.get("name") or "").lower()
        if any(fmt in name_lower for fmt in geo_formats):
            return True
            
        return False

    def view_template(self, context, data_dict):
        return "dataviewer/sdgmap.html"

    def setup_template_variables(self, context, data_dict):
        # Pass the resource information to the template
        # The template will embed the SDGMap in an iframe
        resource = data_dict["resource"]
        
        # SDGMap base URL
        sdgmap_url = "https://sdigmap.tee.gov.gr/sdmquery/public/"
        
        # Add SDGMap URL to template variables
        data_dict["sdgmap_url"] = sdgmap_url
        data_dict["resource_url"] = resource.get("url", "")
        data_dict["resource_format"] = resource.get("format", "").lower()

    # ITemplateHelpers
    def get_helpers(self):
        return {
            "get_common_map_config_sdgmap": utils.get_common_map_config,
        }


class KtimatologioView(GeoViewBase):
    """Custom view for integrating Greek Ktimatologio service"""
    p.implements(p.ITemplateHelpers, inherit=True)

    # IResourceView
    def info(self):
        return {
            "name": "ktimatologio_view",
            "title": "Ψηφιακός Χάρτης Κτηματολογίου",
            "icon": "map-marker",
            "iframed": True,
            "default_title": p.toolkit._("Ψηφιακός Χάρτης Κτηματολογίου"),
        }

    def can_view(self, data_dict):
        # This view can be used for any geographic resource
        # but will be manually selected by users when they want Ktimatologio integration
        resource = data_dict["resource"]
        format_lower = resource.get("format", "").lower()
        
        # Accept common geographic formats that could benefit from Ktimatologio integration
        geo_formats = ["kml", "kmz", "geojson", "shp", "shapefile", "gml", "wms", "wmts"]
        
        if format_lower in geo_formats:
            return True
        
        # Also check if the resource name contains geographic indicators
        name_lower = (resource.get("name") or "").lower()
        if any(fmt in name_lower for fmt in geo_formats):
            return True
            
        return False

    def view_template(self, context, data_dict):
        return "dataviewer/ktimatologio.html"

    def setup_template_variables(self, context, data_dict):
        resource = data_dict['resource']
        
        data_dict["resource_url"] = resource.get("url", "")
        data_dict["resource_format"] = resource.get("format", "").lower()
        data_dict["ktimatologio_url"] = "https://maps.ktimatologio.gr/?locale=el"
        data_dict["auto_load"] = False  # Always manual mode

    # ITemplateHelpers
    def get_helpers(self):
        return {
            "get_common_map_config_ktimatologio": utils.get_common_map_config,
        }

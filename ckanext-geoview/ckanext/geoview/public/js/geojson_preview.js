// geojson preview module
ckan.module('geojsonpreview', function (jQuery, _) {
  return {
    options: {
      table: '<div class="table-container"><table class="table table-striped table-bordered table-condensed"><tbody>{body}</tbody></table></div>',
      row:'<tr><th>{key}</th><td>{value}</td></tr>',
      style: {
        opacity: 0.7,
        fillOpacity: 0.1,
        weight: 2
      },
      i18n: {
        'error': _('An error occurred: %(text)s %(error)s'),
        'file_too_big': _('This GeoJSON file is too big to be previewed. Please download it locally.'),
        'feature_limit_truncated': _('Only %(rendered)s of %(total)s GeoJSON features are shown to keep the browser responsive. Please download the resource for the full dataset.'),
        'coordinate_limit_truncated': _('Only part of this GeoJSON is shown because the preview is limited to %(max)s coordinate positions. Please download the resource for the full dataset.'),
        'geojson_too_complex': _('This GeoJSON is too complex to preview safely. Please download it locally.')
      }
    },
    initialize: function () {
      var self = this;
      var maxFileSize = self.parsePositiveInteger(this.options.max_file_size);

      self.el.empty();

      if (maxFileSize !== null && preload_resource.size &&
        preload_resource.size > maxFileSize) {
        self.showFileTooBigError();
        return
      }



      self.el.append($("<div></div>").attr("id","map"));
      self.map = ckan.commonLeafletMap('map', this.options.map_config, {attributionControl: false});

      // hack to make leaflet use a particular location to look for images
      L.Icon.Default.imagePath = this.options.site_url + 'js/vendor/leaflet/images/';

      // GeoServer may return GeoJSON using either a short EPSG code
      // or OGC URN/URL CRS names. Register common aliases for proj4leaflet.
      var epsg2100 = '+proj=tmerc +lat_0=0 +lon_0=24 +k=0.9996 ' +
        '+x_0=500000 +y_0=0 +ellps=GRS80 ' +
        '+towgs84=-199.87,74.79,246.62,0,0,0,0 +units=m +no_defs +type=crs';
      proj4.defs('EPSG:2100', epsg2100);
      proj4.defs('urn:ogc:def:crs:EPSG::2100', epsg2100);
      proj4.defs('http://www.opengis.net/gml/srs/epsg.xml#2100', epsg2100);

      var epsg4258 = '+proj=longlat +ellps=GRS80 +no_defs +type=crs';
      proj4.defs('EPSG:4258', epsg4258);
      proj4.defs('urn:ogc:def:crs:EPSG::4258', epsg4258);
      proj4.defs('http://www.opengis.net/gml/srs/epsg.xml#4258', epsg4258);

      // The standard CRS for GeoJSON according to RFC 7946 is
      // urn:ogc:def:crs:OGC::CRS84, but proj4s uses a different name
      // for it. See https://github.com/ckan/ckanext-geoview/issues/51
      proj4.defs['OGC:CRS84'] = proj4.defs['EPSG:4326'];
      
      // Use proxy_url if available (from template_variables), otherwise use original URL
      var resourceUrl = (preload_resource['proxy_url']) ? preload_resource['proxy_url'] : preload_resource['url'];
      
      jQuery.ajax({
        url: resourceUrl,
        dataType: 'text'
      }).done(
        function(responseText){
          var data;

          if (maxFileSize !== null &&
              self.getTextByteLength(responseText) > maxFileSize) {
            self.showFileTooBigError();
            return;
          }

          try {
            data = JSON.parse(responseText.replace(/^\uFEFF/, ''));
          } catch (error) {
            self.showError({responseText: ''}, 'parsererror', error.message || error);
            return;
          }

          self.showPreview(data);
        })
      .fail(
        function(jqXHR, textStatus, errorThrown) {
          self.showError(jqXHR, textStatus, errorThrown);
        }
      );
    },

    showError: function (jqXHR, textStatus, errorThrown) {
      if (textStatus == 'error' && jqXHR.responseText.length) {
        this.el.html(jqXHR.responseText);
      } else {
        this.el.html(this.i18n('error', {text: textStatus, error: errorThrown}));
      }
    },

    getTextByteLength: function (text) {
      if (window.Blob) {
        return new Blob([text]).size;
      }

      return text ? text.length : 0;
    },

    showFileTooBigError: function () {
      this.el.html(
        jQuery('<div class="data-viewer-error"><p class="text-danger"></p></div>')
          .find('p')
          .text(this.i18n('file_too_big'))
          .end()
      );
    },

    parsePositiveInteger: function (value) {
      if (value === null || typeof value === 'undefined' || value === '' ||
          value === 'None' || value === false) {
        return null;
      }

      var parsed = parseInt(value, 10);
      return isNaN(parsed) || parsed <= 0 ? null : parsed;
    },

    countCoordinatePositions: function (coordinates) {
      var self = this;

      if (!coordinates) {
        return 0;
      }

      if (typeof coordinates[0] === 'number') {
        return 1;
      }

      var total = 0;
      jQuery.each(coordinates, function (idx, coordinateSet) {
        total += self.countCoordinatePositions(coordinateSet);
      });
      return total;
    },

    countGeometryCoordinatePositions: function (geometry) {
      var self = this;
      var total = 0;

      if (!geometry) {
        return 0;
      }

      if (geometry.type === 'GeometryCollection') {
        jQuery.each(geometry.geometries || [], function (idx, childGeometry) {
          total += self.countGeometryCoordinatePositions(childGeometry);
        });
        return total;
      }

      return self.countCoordinatePositions(geometry.coordinates);
    },

    countFeatureCoordinatePositions: function (feature) {
      if (!feature) {
        return 0;
      }

      return this.countGeometryCoordinatePositions(
        feature.type === 'Feature' ? feature.geometry : feature
      );
    },

    limitGeoJSON: function (geojsonFeature) {
      var maxFeatures = this.parsePositiveInteger(this.options.max_features);
      var maxCoordinates = this.parsePositiveInteger(this.options.max_coordinates);
      var result = {
        data: geojsonFeature,
        originalFeatureCount: null,
        renderedFeatureCount: null,
        maxFeatures: maxFeatures,
        maxCoordinates: maxCoordinates,
        featureLimitReached: false,
        coordinateLimitReached: false,
        tooComplex: false
      };

      if (!geojsonFeature) {
        return result;
      }

      if (geojsonFeature.type === 'FeatureCollection' &&
          jQuery.isArray(geojsonFeature.features)) {
        var features = geojsonFeature.features;
        var renderedFeatures = [];
        var renderedCoordinates = 0;

        result.originalFeatureCount = features.length;

        for (var i = 0; i < features.length; i++) {
          if (maxFeatures !== null && renderedFeatures.length >= maxFeatures) {
            result.featureLimitReached = true;
            break;
          }

          var featureCoordinateCount = this.countFeatureCoordinatePositions(features[i]);
          if (maxCoordinates !== null &&
              renderedCoordinates + featureCoordinateCount > maxCoordinates) {
            result.coordinateLimitReached = true;
            break;
          }

          renderedFeatures.push(features[i]);
          renderedCoordinates += featureCoordinateCount;
        }

        result.renderedFeatureCount = renderedFeatures.length;
        result.tooComplex = features.length > 0 && renderedFeatures.length === 0;
        result.data = jQuery.extend({}, geojsonFeature, {features: renderedFeatures});
        return result;
      }

      if (maxCoordinates !== null &&
          this.countFeatureCoordinatePositions(geojsonFeature) > maxCoordinates) {
        result.coordinateLimitReached = true;
        result.tooComplex = true;
      }

      return result;
    },

    showLimitWarning: function (limitResult) {
      var messages = [];

      if (limitResult.featureLimitReached) {
        messages.push(this.i18n('feature_limit_truncated', {
          rendered: limitResult.renderedFeatureCount,
          total: limitResult.originalFeatureCount
        }));
      }

      if (limitResult.coordinateLimitReached && !limitResult.tooComplex) {
        messages.push(this.i18n('coordinate_limit_truncated', {
          max: limitResult.maxCoordinates
        }));
      }

      if (messages.length) {
        this.el.append(
          jQuery('<div class="geojson-preview-warning alert alert-warning"></div>')
            .text(messages.join(' '))
        );
      }
    },

    showLimitError: function () {
      this.el.html(
        jQuery('<div class="data-viewer-error"><p class="text-danger"></p></div>')
          .find('p')
          .text(this.i18n('geojson_too_complex'))
          .end()
      );
    },

    showPreview: function (geojsonFeature) {
      var self = this;
      var limitResult = self.limitGeoJSON(geojsonFeature);

      if (limitResult.tooComplex) {
        self.showLimitError();
        return;
      }

      self.showLimitWarning(limitResult);

      var gjLayer = L.Proj.geoJson(limitResult.data, {
        style: self.options.style,
        onEachFeature: function(feature, layer) {
          var body = '';
          if (feature.properties) {
            jQuery.each(feature.properties, function(key, value){
              if (value != null && typeof value === 'object') {
                value = JSON.stringify(value);
              }
              body += L.Util.template(self.options.row, {key: key, value: value});
            });
            var popupContent = L.Util.template(self.options.table, {body: body});
            layer.bindPopup(popupContent);
          }
        }
      }).addTo(self.map);

      var bounds = gjLayer.getBounds();
      if (bounds && bounds.isValid()) {
        self.map.fitBounds(bounds);
      }
    }
  };
});

/* global ckan, OL_HELPERS, ol, $, preload_resource */
// Openlayers preview module

(function() {

    if (window.Proj4js) {
        // add your projection definitions here
        // definitions can be found at http://spatialreference.org/ref/epsg/{xxxx}/proj4js/

    }

    var $_ = _ // keep pointer to underscore, as '_' will may be overridden by a closure variable when down the stack

    // helper function for safe JSON parsing
    function safeParse(v, fallback) {
      if (v == null) return fallback;
      if (typeof v === 'object') return v;
      if (typeof v !== 'string') return fallback;
      try { return JSON.parse(v); } catch (e) { return v; }  // if not JSON, keep the string
    }

    // deeper parse for cases where JSON is double-encoded (eg, "\"{...}\"")
    function deepParse(v, fallback) {
      var out = safeParse(v, fallback);
      if (typeof out === 'string') {
        try { out = JSON.parse(out); } catch (e) { /* ignore */ }
      }
      return out;
    }

    // Determine if a URL is same-origin with the current page
    function isSameOrigin(url) {
      try {
        var loc = window.location;
        var base = loc.protocol + '//' + loc.host;
        var u = new URL(url, base);
        return (u.protocol === loc.protocol && u.host === loc.host);
      } catch (e) {
        return false;
      }
    }

    this.ckan.module('olpreview', function (jQuery, _) {

        ckan.geoview = ckan.geoview || {}

        var esrirestExtractor = function(resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
            var parsedUrl = resource.url.split('#');
            var url = proxyServiceUrl || parsedUrl[0];

            var layerName = parsedUrl.length > 1 && parsedUrl[1];

            OL_HELPERS.withArcGisLayers(url, layerProcessor, layerName, parsedUrl[0]);
        }

        ckan.geoview.layerExtractors = {

            'kml': function (resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                // Always prefer proxyUrl if provided to avoid CORS on redirects (e.g., Azure SAS)
                var url = proxyUrl || resource.url;
                layerProcessor(OL_HELPERS.createKMLLayer(url));
            },
            'gml': function (resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                var url = proxyUrl || resource.url;
                layerProcessor(OL_HELPERS.createGMLLayer(url));
            },
            'geojson': function (resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                var url = proxyUrl || resource.url;
                layerProcessor(OL_HELPERS.createGeoJSONLayer(url));
            },
            'wfs': function(resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                var parsedUrl = resource.url.split('#');
                var url = proxyServiceUrl || parsedUrl[0];

                var ftName = parsedUrl.length > 1 && parsedUrl[1];
                OL_HELPERS.withFeatureTypesLayers(url, layerProcessor, ftName, map, true /* useGET */);
            },
            'wms' : function(resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                var parsedUrl = resource.url.split('#');
                // use the original URL for the getMap, as there's no need for a proxy for image requests
                var getMapUrl = parsedUrl[0];

                var url = proxyServiceUrl || getMapUrl;

                var layerName = parsedUrl.length > 1 && parsedUrl[1];
                OL_HELPERS.withWMSLayers(url, getMapUrl, layerProcessor, layerName, true /* useTiling*/, map );
            },
            'wmts' : function(resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                var parsedUrl = resource.url.split('#');

                var url = proxyServiceUrl || parsedUrl[0];

                var layerName = parsedUrl.length > 1 && parsedUrl[1];
                OL_HELPERS.withWMTSLayers(url, layerProcessor, layerName);
            },
            'esrigeojson': function (resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                var url = proxyUrl || resource.url;
                layerProcessor(OL_HELPERS.createEsriGeoJSONLayer(url));
            },
            'arcgis_rest': esrirestExtractor ,
            'esri rest': esrirestExtractor ,
            'gft': function (resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {
                var tableId = OL_HELPERS.parseURL(resource.url).query.docid;
                layerProcessor(OL_HELPERS.createGFTLayer(tableId, ckan.geoview.gapi_key));
            }
        }

        var withLayers = function (resource, proxyUrl, proxyServiceUrl, layerProcessor, map) {

            var withLayers = ckan.geoview.layerExtractors[resource.format && resource.format.toLocaleLowerCase()];
            withLayers && withLayers(resource, proxyUrl, proxyServiceUrl, layerProcessor, map);
        }

        return {
            options: {
                i18n: {
                }
            },

            initialize: function () {
                jQuery.proxyAll(this, /_on/);
                this.el.ready(this._onReady);
            },

            addLayer: function (resourceLayer) {
                if (resourceLayer.setStyle) {
                  resourceLayer.setStyle(this.defaultStyle);
                }

                if (this.options.ol_config.hide_overlays &&
                    this.options.ol_config.hide_overlays.toLowerCase() == "true") {
                    resourceLayer.setVisibility(false);
                }

                this.map.addLayerWithExtent(resourceLayer);
            },

            _commonBaseLayer: function(mapConfig, callback, module) {

                if (mapConfig.type == 'mapbox') {
                    // MapBox base map
                    if (!mapConfig['map_id'] || !mapConfig['access_token']) {
                      throw '[CKAN Map Widgets] You need to provide a map ID ([account].[handle]) and an access token when using a MapBox layer. ' +
                            'See http://www.mapbox.com/developers/api-overview/ for details';
                    }

                    mapConfig.url = ['//a.tiles.mapbox.com/v4/' + mapConfig['map_id'] + '/${z}/${x}/${y}.png?access_token=' + mapConfig['access_token'],
                                '//b.tiles.mapbox.com/v4/' + mapConfig['map_id'] + '/${z}/${x}/${y}.png?access_token=' + mapConfig['access_token'],
                                '//c.tiles.mapbox.com/v4/' + mapConfig['map_id'] + '/${z}/${x}/${y}.png?access_token=' + mapConfig['access_token'],
                                '//d.tiles.mapbox.com/v4/' + mapConfig['map_id'] + '/${z}/${x}/${y}.png?access_token=' + mapConfig['access_token'],
                    ];
                    mapConfig.attribution = '<a href="https://www.mapbox.com/about/maps/" target="_blank">&copy; Mapbox &copy; OpenStreetMap </a> <a href="https://www.mapbox.com/map-feedback/" target="_blank">Improve this map</a>';

                } else if (mapConfig.type == 'custom') {
                    mapConfig.type = 'XYZ'
                } else if (!mapConfig.type || mapConfig.type.toLowerCase() == 'osm') {

                    mapConfig.type = 'OSM'
                }

                return OL_HELPERS.createLayerFromConfig(mapConfig, true).then(callback);
            },

            createMapFun: function (baseMapLayerList, overlays) {

                var layerSwitcher = new ol.control.HilatsLayerSwitcher();

                var styleMapJson = OL_HELPERS.DEFAULT_STYLEMAP;

                if (ckan.geoview && ckan.geoview.feature_style) {
                    styleMapJson = safeParse(ckan.geoview.feature_style, styleMapJson);
                    // default style can be json w/ expressions, highlight style needs to be objectified
                    if (styleMapJson.highlight) {
                        // must convert highlight style to objects.
                        styleMapJson.highlight = OL_HELPERS.makeStyle(styleMapJson.highlight);
                    }
                }
                this.defaultStyle = styleMapJson.default || styleMapJson;
                this.highlightStyle = styleMapJson.highlight || undefined;


                var coordinateFormatter = function(coordinate) {
                    var degrees = map && map.getView() && map.getView().getProjection() && (map.getView().getProjection().getUnits() == 'degrees')
                    return ol.coordinate.toStringXY(coordinate, degrees ? 5:2);
                };

                const baseMapLayer = baseMapLayerList[0];

                var options = {
                    target: $('.map')[0],
                    layers: baseMapLayerList,
                    controls: [
                        new ol.control.ZoomSlider(),
                        new ol.control.MousePosition( {
                            coordinateFormat: coordinateFormatter,
                        }),
                        layerSwitcher
                    ],
                    loadingDiv: false,
                    loadingListener: function(isLoading) {
                        layerSwitcher.isLoading(isLoading);
                    },
                    overlays: overlays,
                    view: new ol.View({
                        // projection attr should be set when creating a baselayer
                        projection: baseMapLayer.getSource().getProjection() || OL_HELPERS.Mercator,
                        extent: baseMapLayer.getExtent(), /* TODO_OL4 is this equivalent to maxExtent? */
                        //center: [0,0],
                        //zoom: 4
                    })
                };

                var map = this.map = new OL_HELPERS.LoggingMap(options);
                // by default stretch the map to the basemap extent or to the world
                map.getView().fit(
                        baseMapLayer.getExtent() || ol.proj.transformExtent(OL_HELPERS.WORLD_BBOX, OL_HELPERS.EPSG4326, map.getView().getProjection()),
                    {constrainResolution: false}
                );

                map.highlightStyle = this.highlightStyle;
                let selected = null;
                map.on('pointermove', function (e) {
                    if (selected !== null) {
                        selected.setStyle(undefined);
                        selected = null;
                    }

                    map.forEachFeatureAtPixel(e.pixel, function (f) {
                        selected = f;
                        f.setStyle(map.highlightStyle);
                        return true;
                    });
                });

                // force a reload of all vector sources on projection change
                map.getView().on('change:projection', function() {
                    map.getLayers().forEach(function(layer) {
                        if (layer instanceof ol.layer.Vector) {
                            layer.getSource().clear();
                        }
                    });
                });
                map.on('change:view', function() {
                    map.getLayers().forEach(function(layer) {
                        if (layer instanceof ol.layer.Vector) {
                            layer.getSource().clear();
                        }
                    });
                });


                var fragMap = OL_HELPERS.parseKVP((window.parent || window).location.hash && (window.parent || window).location.hash.substring(1));

                var bbox = fragMap.bbox && fragMap.bbox.split(',').map(parseFloat)
                var bbox = bbox && ol.proj.transformExtent(bbox, OL_HELPERS.EPSG4326, this.map.getProjection());
                if (bbox) this.map.zoomToExtent(bbox);

                /* Update URL with current bbox
                var $map = this.map;
                var mapChangeListener = function() {
                    var newBbox = $map.getExtent() && $map.getExtent().transform($map.getProjectionObject(), OL_HELPERS.EPSG4326).toString()

                    if (newBbox) {
                        var fragMap = OL_HELPERS.parseKVP((window.parent || window).location.hash && (window.parent || window).location.hash.substring(1));
                        fragMap['bbox'] = newBbox;

                        (window.parent || window).location.hash = OL_HELPERS.kvp2string(fragMap)
                    }
                }


                // listen to bbox changes to update URL fragment
                this.map.events.register("moveend", this.map, mapChangeListener);

                this.map.events.register("zoomend", this.map, mapChangeListener);

                */


                var proxyUrl = this.options.proxy_url;
                var proxyServiceUrl = this.options.proxy_service_url;

                ckan.geoview.googleApiKey = this.options.gapi_key;


                withLayers(preload_resource, proxyUrl, proxyServiceUrl, $_.bind(this.addLayer, this), this.map);
            },

            _onReady: function () {

                var baseMapsConfig = this.options.basemapsConfig

                // gather options and config for this view
                var proxyUrl = this.options.proxy_url;
                var proxyServiceUrl = this.options.proxy_service_url;

                // Process options with safeParse and normalize option names
                // Normalize underscore forms to camelCase if present
                if (!this.options.siteUrl && this.options.site_url) this.options.siteUrl = this.options.site_url;
                if (!this.options.proxyUrl && this.options.proxy_url) this.options.proxyUrl = this.options.proxy_url;
                if (!this.options.proxyServiceUrl && this.options.proxy_service_url) this.options.proxyServiceUrl = this.options.proxy_service_url;
                if (!this.options.rawUrl && this.options.raw_url) this.options.rawUrl = this.options.raw_url;
                if (!this.options.resourceView && this.options['resource-view']) this.options.resourceView = this.options['resource-view'];

                this.options.siteUrl = safeParse(this.options.siteUrl, this.options.siteUrl);
                this.options.proxyUrl = safeParse(this.options.proxyUrl, this.options.proxyUrl);
                this.options.proxyServiceUrl = safeParse(this.options.proxyServiceUrl, this.options.proxyServiceUrl);
                this.options.map_config = safeParse(this.options.map_config, this.options.map_config || {});
                this.options.ol_config = safeParse(this.options.ol_config, this.options.ol_config || {});

                // Mirror back to underscore keys so downstream code picks them up
                if (!this.options.site_url && this.options.siteUrl) this.options.site_url = this.options.siteUrl;
                if (!this.options.proxy_url && this.options.proxyUrl) this.options.proxy_url = this.options.proxyUrl;
                if (!this.options.proxy_service_url && this.options.proxyServiceUrl) this.options.proxy_service_url = this.options.proxyServiceUrl;
                if (!this.options.raw_url && this.options.rawUrl) this.options.raw_url = this.options.rawUrl;
                
                if (this.options.resourceView) {
                    var resourceView = deepParse(this.options.resourceView, {});
                    this.options.resourceView = resourceView;
                    $_.extend(ckan.geoview, resourceView);
                    
                    // If proxyServiceUrl is not provided, build it from resourceView data
                    if (!this.options.proxyServiceUrl && resourceView && resourceView.package_id && resourceView.resource_id && this.options.siteUrl) {
                        // Make sure siteUrl has a trailing slash
                        var base = typeof this.options.siteUrl === 'string' ? this.options.siteUrl : (this.options.siteUrl || '');
                        if (base && !base.endsWith('/')) base += '/';
                        this.options.proxyServiceUrl = base + 'dataset/' + resourceView.package_id + '/resource/' + resourceView.resource_id + '/resource_proxy';
                    }
                    
                    // If it's KML/KMZ, prefer the dedicated file proxy when the URL is a CKAN uploaded download
                    var rawUrl = this.options.rawUrl || (preload_resource && preload_resource.url);
                    if (rawUrl && /(\.kml|\.kmz)(\?|$)/i.test(rawUrl)) {
                        var rv = this.options.resourceView || (this.options['resource-view'] && deepParse(this.options['resource-view'])) || {};
                        var siteBase = this.options.siteUrl || this.options.site_url || '';
                        if (typeof siteBase === 'string' && siteBase && !siteBase.endsWith('/')) siteBase += '/';
                        try {
                            var parsed = new URL(rawUrl, siteBase || window.location.origin);
                            var pth = parsed.pathname || '';
                            var fname = null;
                            var dlIdx = pth.lastIndexOf('/download/');
                            if (dlIdx >= 0) {
                                fname = pth.substring(dlIdx + '/download/'.length);
                            } else {
                                fname = pth.substring(pth.lastIndexOf('/') + 1);
                            }
                            if (fname) fname = decodeURIComponent(fname);

                            if (rv && rv.resource_id && fname && siteBase && dlIdx >= 0) {
                                // Use our server-side file proxy which streams directly from Azure; mirror to underscore key as well
                                this.options.proxyUrl = siteBase + 'geoview_file_proxy/' + rv.resource_id + '/' + fname;
                                this.options.proxy_url = this.options.proxyUrl;
                            } else if (this.options.proxyServiceUrl) {
                                // Fallback to generic proxy service if we couldn't build the file proxy URL; mirror underscore key
                                this.options.proxyUrl = this.options.proxyServiceUrl + '?url=' + encodeURIComponent(rawUrl);
                                this.options.proxy_url = this.options.proxyUrl;
                            }
                        } catch (e) {
                            // If URL parsing fails, fallback to generic proxy if available; mirror underscore key
                            if (this.options.proxyServiceUrl) {
                                this.options.proxyUrl = this.options.proxyServiceUrl + '?url=' + encodeURIComponent(rawUrl);
                                this.options.proxy_url = this.options.proxyUrl;
                            }
                        }
                    }
                }

                ckan.geoview.gapi_key = this.options.gapi_key;

                var mapDiv = $("<div></div>").attr("id", "map").addClass("map")
                var info = $("<div></div>").attr("id", "info")
                mapDiv.append(info)

                $("#map-container").empty()
                $("#map-container").append(mapDiv)

                info.tooltip({
                    animation: false,
                    trigger: 'manual',
                    placement: "right",
                    html: true
                });

                var overlays = []
                if ((ckan.geoview && 'feature_hoveron' in ckan.geoview) ? ckan.geoview['feature_hoveron'] : this.options.ol_config.default_feature_hoveron)
                    overlays.push(new OL_HELPERS.FeatureInfoOverlay({
                        element: $("<div class='popupContainer'><div class='popupContent'></div></div>")[0],
                        autoPan: false,
                        offset: [5,5]
                    }))



                var $this = this;

                // Choose base map based on CKAN wide config

                if (!baseMapsConfig) {
                    // deprecated - for backward comp, parse old config format into json config
                    var config = {
                        type: this.options.map_config['type']
                    }
                    var prefix = config.type+'.'
                    for (var fieldName in this.options.map_config) {
                        if (fieldName.startsWith(prefix)) config[fieldName.substring(prefix.length)] = this.options.map_config[fieldName]
                    }
                    baseMapsConfig = [config]
                }

                this._commonBaseLayer(
                    baseMapsConfig[0],
                    function(layer) {
                        baseMapsConfig[0].$ol_layer = layer;
                        $this.createMapFun(layer, overlays);

                        // add all configured basemap layers
                        if (baseMapsConfig.length > 1) {
                            // add other basemaps if any
                            for (var idx=1;idx<baseMapsConfig.length;idx++) {
                                OL_HELPERS.createLayerFromConfig(
                                    baseMapsConfig[idx],
                                    true,
                                    function(layer) {
                                        layer.setVisible(false)
                                        // insert all basemaps at the bottom
                                        $this.map.getLayers().insertAt(0, layer)
                                    });
                            }
                        }
                    },
                    this);

            }
        }
    });
})();

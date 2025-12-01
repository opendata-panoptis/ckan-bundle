from setuptools import setup, find_packages

version = '0.2.2'

setup(
    name='ckanext-geoview',
    version=version,
    description='Geospatial viewers for CKAN resources',
    long_description='This extension contains view plugins to display geospatial files and services in CKAN.',
    long_description_content_type='text/plain',
    classifiers=[],
    keywords='',
    author='Philippe Duchesne, Adrià Mercader and contributors',
    author_email='tech-team@ckan.org',
    url='http://github.com/ckan/ckanext-geoview',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points='''
    [ckan.plugins]
    geo_view=ckanext.geoview.plugin:OLGeoView
    geojson_view=ckanext.geoview.plugin:GeoJSONView
    wmts_view=ckanext.geoview.plugin:WMTSView
    shp_view=ckanext.geoview.plugin:SHPView
    sdgmap_view=ckanext.geoview.plugin:SDGMapView
    ktimatologio_view=ckanext.geoview.plugin:KtimatologioView
    ''',
)

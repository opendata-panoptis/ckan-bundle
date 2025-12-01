from setuptools import setup, find_packages

setup(
    name='ckanext-athens-theme',
    version='0.1.0',
    description='CKAN theme for Municipality of Athens',
    long_description='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='CKAN Athens Municipality theme',
    author='Municipality of Athens',
    author_email='info@cityofathens.gr',
    url='https://github.com/cityofathens/ckanext-athens-theme',
    license='AGPL',
    packages=find_packages(include=['ckanext', 'ckanext.athens_theme']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    setup_requires=[
        'babel'
    ],
    install_requires=[],
    entry_points='''
        [ckan.plugins]
        athens_theme=ckanext.athens_theme.plugin:AthensThemePlugin

        [babel.extractors]
        ckan = ckan.lib.extract:extract_ckan
    ''',
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
            ('**.json', 'json', None),
        ],
    },
)
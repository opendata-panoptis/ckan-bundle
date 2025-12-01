from setuptools import setup, find_packages

setup(
    name='ckanext-azure-translator',
    version='0.0.1',
    description='CKAN extension to translate content using Azure Translator',
    author='Your Name',
    author_email='your@email.com',
    license='AGPL',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points='''
        [ckan.plugins]
        azure_translator=ckanext.azure_translator.plugin:AzureTranslatorPlugin
    ''',
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    }
)
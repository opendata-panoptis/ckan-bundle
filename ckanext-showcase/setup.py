from setuptools import setup, find_packages
from setuptools.command.develop import develop
from os import path
import os
import subprocess

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

class DevelopWithTranslations(develop):
    def run(self):
        # Run standard develop command first
        super().run()

        i18n_dir = os.path.join('ckanext', 'showcase', 'i18n')
        if not os.path.isdir(i18n_dir):
            print(f"[i18n] Directory '{i18n_dir}' not found. Skipping translation compilation.")
            return

        try:
            print(f"[i18n] Compiling translations in {i18n_dir}...")
            subprocess.check_call([
                'python', 'setup.py', 'compile_catalog',
                '-d', i18n_dir,
                '-f'  # Force overwrite existing .mo files
            ])
        except subprocess.CalledProcessError as e:
            print(f"[i18n] Error compiling translations: {e}")

setup(
    name='ckanext-showcase',
    version='1.8.1',
    description='A ckan extension to showcase datasets in use',
    long_description=long_description,
    url='https://github.com/ckan/ckanext-showcase',
    author='Brook Elgie',
    author_email='brook.elgie@okfn.org',
    license='AGPL',
    classifiers=[
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='ckan',
    packages=find_packages(include=['ckanext', 'ckanext.*']),
    install_requires=[],
    include_package_data=True,
    package_data={},
    data_files=[],
    entry_points='''
        [ckan.plugins]
        showcase=ckanext.showcase.plugin:ShowcasePlugin

        [babel.extractors]
        ckan = ckan.lib.extract:extract_ckan

    ''',

    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    },
    cmdclass={
        'develop': DevelopWithTranslations,
    }
)

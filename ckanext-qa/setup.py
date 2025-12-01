from setuptools import setup, find_packages
from setuptools.command.develop import develop
import os
import subprocess

class DevelopWithTranslations(develop):
    def run(self):
        # Run standard develop command first
        super().run()

        i18n_dir = os.path.join('ckanext', 'qa', 'i18n')
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
    name='ckanext-qa',
    version='2.0',
    description='Quality Assurance plugin for CKAN',
    author='Open Knowledge Foundation, Cabinet Office & contributors',
    author_email='info@okfn.org',
    maintainer='CKAN Tech Team and contributors',
    maintainer_email='tech-team@ckan.org',
    url='http://github.com/ckan/ckanext-qa',
    packages=find_packages(),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'rdflib>=6.0.0',
        'python-magic>=0.4.24',
        'tldextract>=3.1.0',
        'SPARQLWrapper>=2.0.0',
    ],
    entry_points={
        'ckan.plugins': [
            'qa=ckanext.qa.plugin:qa',
        ],
        'ckan.migrations': [
            'qa=ckanext.qa.migration.qa:alembic_ini',
        ],
    },
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    cmdclass={
        'develop': DevelopWithTranslations,
    }
)
from setuptools import setup, find_packages
from setuptools.command.develop import develop
import os
import subprocess

version = '0.1'

class DevelopWithTranslations(develop):
    def run(self):
        # Run standard develop command first
        super().run()

        i18n_dir = os.path.join('ckanext', 'report', 'i18n')
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
    name='ckanext-report',
    version=version,
    description="Framework for defining reports in CKAN",
    long_description='''
    ''',
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='David Read',
    author_email='david.read@hackneyworkshop.com',
    url='',
    license='Affero General Public License',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.report'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points='''
        [ckan.plugins]
        report = ckanext.report.plugin:ReportPlugin
        tagless_report = ckanext.report.plugin:TaglessReportPlugin

        [paste.paster_command]
        report = ckanext.report.command:ReportCommand
    ''',
    cmdclass={
        'develop': DevelopWithTranslations,
    }
)
# -*- coding: utf-8 -*-
from setuptools import setup
from setuptools.command.develop import develop
import os
import subprocess

class DevelopWithTranslations(develop):
    def run(self):
        # Run standard develop command first
        super().run()

        i18n_dir = os.path.join('ckanext', 'vocabulary_admin', 'i18n')
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

# Note: Do not add new arguments to setup(), instead add setuptools
# configuration options to setup.cfg, or any other project information
# to pyproject.toml
# See https://github.com/ckan/ckan/issues/8382 for details

setup(
    # If you are changing from the default layout of your extension, you may
    # have to change the message extractors, you can read more about babel
    # message extraction at
    # http://babel.pocoo.org/docs/messages/#extraction-method-mapping-and-configuration
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

# -*- coding: utf-8 -*-
from setuptools import setup
from setuptools.command.develop import develop
import os
import subprocess

class DevelopWithTranslations(develop):
    def run(self):
        # Run standard develop command first
        super().run()

        i18n_dir = os.path.join('ckanext', 'data_gov_gr', 'i18n')
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

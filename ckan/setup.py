# encoding: utf-8

import os
from setuptools import setup
from setuptools.command.install import install

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        self.compile_translations()

    def compile_translations(self):
        """Compile translations after installation"""
        import subprocess
        import glob

        # Find all .po files
        po_files = glob.glob('ckan/i18n/*/LC_MESSAGES/*.po')

        for po_file in po_files:
            mo_file = po_file[:-3] + '.mo'
            try:
                subprocess.run(['msgfmt', po_file, '-o', mo_file],
                             check=True, capture_output=True)
                print(f"Compiled {po_file}")
            except subprocess.CalledProcessError as e:
                print(f"Failed to compile {po_file}: {e}")

# Avoid problem releasing to pypi from vagrant
if os.environ.get("USER", "") == "vagrant":
    del os.link

extras_require = {}
_extras_groups = [
    ("requirements", "requirements.txt"),
    ("dev", "dev-requirements.txt"),
]

HERE = os.path.dirname(__file__)
for group, filepath in _extras_groups:
    with open(os.path.join(HERE, filepath), "r") as f:
        extras_require[group] = f.readlines()

setup(
    message_extractors={
        "ckan": [
            ("**.py", "python", None),
            ("**.js", "javascript", None),
            ("templates/**.html", "ckan", None),
            ("templates/**.txt", "ckan", None),
            ("public/**", "ignore", None),
        ],
        "ckanext": [
            ("**.py", "python", None),
            ("**.js", "javascript", None),
            ("**.html", "ckan", None),
            ("multilingual/solr/*.txt", "ignore", None),
        ],
    },
    extras_require=extras_require,
    cmdclass={
        'install': PostInstallCommand,
        'develop': PostInstallCommand,
    }

)

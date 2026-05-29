.. You should enable this project on travis-ci.org and coveralls.io to make
   these badges work. The necessary Travis and Coverage config files have been
   generated for you.

.. image:: https://github.com/ckan/ckanext-showcase/workflows/Tests/badge.svg?branch=master
    :target: https://github.com/ckan/ckanext-showcase/actions

.. image:: https://codecov.io/gh/ckan/ckanext-showcase/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/ckan/ckanext-showcase

================
ckanext-showcase
================

Showcase and link to datasets in use. Datasets used in an app, website or
visualization, or featured in an article, report or blog post can be showcased
within the CKAN website. Showcases can include an image, description, tags and
external link. Showcases may contain several datasets, helping users discover
related datasets being used together. Showcases can be discovered by searching
and filtered by tag.

Site sysadmins can promote selected users to become 'Showcase Admins' to help
create, populate and maintain showcases.

ckanext-showcase is intended to be a more powerful replacement for the
'Related Item' feature.

---

Συγκεκριμένη υλοποίηση στο plugin επεκτείνει τον μηχανισμό αποστολής email ειδοποιήσεων που εκτελείται **αποκλειστικά κατά τη δημιουργία νέας Showcase εφαρμογής** (`showcase_create`) και εισάγει παραμετρική διαχείριση παραληπτών μέσω ρυθμίσεων στο `ckan.ini`.

Πιο συγκεκριμένα, δίνεται η δυνατότητα επιλογής του τρόπου υπολογισμού των παραληπτών των ειδοποιήσεων κατά το create, με υποστήριξη των εξής περιπτώσεων:

* αποστολή στο σύνολο των `sysadmins` και `showcase admins`
* αποστολή μόνο στους `showcase admins`
* αποστολή μόνο σε ένα συγκεκριμένο email που ορίζεται παραμετρικά

Επιπλέον, προστέθηκε δυνατότητα ορισμού ενός σταθερού email παραλήπτη, το οποίο μπορεί να συμμετέχει στις αποστολές όταν είναι δηλωμένο, καθώς και δυνατότητα ορισμού λίστας εξαιρούμενων email, ώστε συγκεκριμένοι χρήστες να αποκλείονται από τις ειδοποιήσεις ακόμα και αν ανήκουν στο σύνολο των υποψήφιων παραληπτών.

Η ανάκτηση των email δεν βασίζεται πλέον στα CKAN actions `user_list` / `user_show`, τα οποία ενδέχεται να μην επιστρέφουν email πεδία λόγω authorization ή privacy restrictions του τρέχοντος context. Αντί αυτού, η υλοποίηση χρησιμοποιεί απευθείας ανάκτηση από τη βάση για μεγαλύτερη αξιοπιστία και σταθερότητα.

Η αλλαγή αυτή **αφορά μόνο τη ροή δημιουργίας Showcase** και δεν επεκτείνεται σε άλλες ενέργειες του plugin, όπως ενημέρωση, έγκριση ή διαγραφή.

**Σημείωση:** Η παραπάνω παραμετροποίηση αφορά τους recipients των ειδοποιήσεων προς διαχειριστές / configured email. Η αποστολή email προς τον δημιουργό του Showcase **παραμένει κανονικά ενεργή**, ανεξάρτητα από αυτή την παραμετροποίηση, όπως ήδη προβλέπεται στην υφιστάμενη ροή δημιουργίας.

## Properties στο `ckan.ini`

Η παραμετροποίηση γίνεται μέσω των παρακάτω properties:

```ini
ckanext.showcase.notification_recipients_mode = all_admins
ckanext.showcase.notification_email = showcase-notify@example.gr
ckanext.showcase.notification_exclude_emails = a@example.gr, b@example.gr
```

Το prefix που εμφανίζεται στα subjects των email ειδοποιήσεων διαβάζεται από
την υπάρχουσα ρύθμιση του CKAN:

```ini
ckan.site_title = Data.gov.gr
```

Αν το `ckan.site_title` δεν έχει οριστεί ή είναι κενό, τα email subjects
αποστέλλονται χωρίς prefix.

## Περιγραφή properties

### `ckanext.showcase.notification_recipients_mode`

Καθορίζει τον βασικό τρόπο επιλογής των παραληπτών. Υποστηρίζονται οι τιμές:

* `all_admins`
  Αποστολή σε όλους τους `sysadmins` και `showcase admins`

* `showcase_admins`
  Αποστολή μόνο στους `showcase admins`

* `configured_email_only`
  Αποστολή μόνο στο email που έχει δηλωθεί στο `ckanext.showcase.notification_email`

### `ckanext.showcase.notification_email`

Ορίζει ένα συγκεκριμένο email παραλήπτη. Όταν είναι δηλωμένο:

* προστίθεται επιπλέον στους παραλήπτες όταν το mode είναι `all_admins` ή `showcase_admins`
* χρησιμοποιείται ως μοναδικός παραλήπτης όταν το mode είναι `configured_email_only`

Αν το property είναι κενό ή δεν έχει οριστεί, τότε αγνοείται.
Στην περίπτωση `configured_email_only`, αν δεν έχει δηλωθεί τιμή, δεν θα προστεθεί configured recipient.

### `ckanext.showcase.notification_exclude_emails`

Λίστα από email διευθύνσεις που θα εξαιρούνται από την αποστολή, ακόμα κι αν προκύπτουν από το επιλεγμένο mode ή από το configured email.

Η λίστα μπορεί να δοθεί ως comma-separated τιμές, π.χ.:

```ini
ckanext.showcase.notification_exclude_emails = user1@example.gr, user2@example.gr
```

## Default συμπεριφορά

Αν δεν οριστεί καμία από τις σχετικές παραμέτρους στο ckan.ini, εφαρμόζεται η προεπιλεγμένη συμπεριφορά του μηχανισμού, δηλαδή αποστολή ειδοποίησης στους sysadmins και showcase admins, χωρίς configured email και χωρίς exclude list. Η αποστολή email προς τον δημιουργό του Showcase παραμένει επίσης ενεργή κανονικά.

## Παραδείγματα χρήσης

### 1. Αποστολή σε sysadmins και showcase admins

```ini
ckanext.showcase.notification_recipients_mode = all_admins
ckanext.showcase.notification_email =
ckanext.showcase.notification_exclude_emails =
```

### 2. Αποστολή μόνο σε showcase admins

```ini
ckanext.showcase.notification_recipients_mode = showcase_admins
ckanext.showcase.notification_email =
ckanext.showcase.notification_exclude_emails =
```

### 3. Αποστολή μόνο σε ένα συγκεκριμένο email

```ini
ckanext.showcase.notification_recipients_mode = configured_email_only
ckanext.showcase.notification_email = showcase-notify@example.gr
ckanext.showcase.notification_exclude_emails =
```

### 4. Αποστολή σε sysadmins και showcase admins, με επιπλέον σταθερό παραλήπτη

```ini
ckanext.showcase.notification_recipients_mode = all_admins
ckanext.showcase.notification_email = showcase-notify@example.gr
ckanext.showcase.notification_exclude_emails =
```

### 5. Αποστολή μόνο σε showcase admins, με αποκλεισμό συγκεκριμένων email

```ini
ckanext.showcase.notification_recipients_mode = showcase_admins
ckanext.showcase.notification_email =
ckanext.showcase.notification_exclude_emails = admin1@example.gr, admin2@example.gr
```

### 6. Αποστολή σε sysadmins και showcase admins, με configured email και exclude list

```ini
ckanext.showcase.notification_recipients_mode = all_admins
ckanext.showcase.notification_email = showcase-notify@example.gr
ckanext.showcase.notification_exclude_emails = admin2@example.gr, showcase-notify@example.gr
```

Στο παραπάνω παράδειγμα:

* θα υπολογιστούν οι παραλήπτες από `sysadmins + showcase admins`
* θα προστεθεί και το `showcase-notify@example.gr`
* στο τέλος θα αφαιρεθούν όσα emails υπάρχουν στη λίστα αποκλεισμού

---

------------
Requirements
------------

Tested on CKAN 2.9 to 2.11.

Note: Use `1.5.2` for older CKAN versions (2.7 and 2.8).

------------
Installation
------------

.. Add any additional install steps to the list below.
   For example installing any non-Python dependencies or adding any required
   config settings.

To install ckanext-showcase:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-showcase Python package into your virtual environment::

     pip install ckanext-showcase

3. Add ``showcase`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Create the database tables::

    ckan db upgrade -p showcase


5. Restart CKAN. 

------------------------
Development Installation
------------------------

To install ckanext-showcase for development, activate your CKAN virtualenv and
do::

    git clone https://github.com/ckan/ckanext-showcase.git
    cd ckanext-showcase
    pip install -e .
    pip install -r dev-requirements.txt


The extension contains a custom build of CKEditor to allow using a WYSIWYG editor
to write the content of the showcase. It has been built using `webpack` and the
repository contains all the files needed to edit and customize it if needed::

    npm install
    npx webpack --config webpack.config.js

Build anatomy
 * assets/build/ckeditor.js - The ready-to-use editor bundle, containing the editor and all plugins.
 * assets/js/showcase-editor - The CKAN module that will load and config the bundle when using it as data-module attribute.
 * assets/src/ckeditor.js - The source entry point of the build. Based on it the build/ckeditor.js file is created by webpack. It defines the editor creator, the list of plugins and the default configuration of a build.
 * webpack.config.js - The webpack configuration used to build the editor.

More info on how to build CKEditor from source:
https://ckeditor.com/docs/ckeditor5/latest/installation/getting-started/quick-start-other.html#building-the-editor-from-source


---
API
---

All actions in the Showcase extension are available in the CKAN Action API.

Showcase actions::

    - create a new showcase (sysadmins and showcase admins only)
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_create -H "Authorization:{YOUR-API-KEY}" -d '{"name": "my-new-showcase"}'

    - delete a showcase (sysadmins and showcase admins only)
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_delete -H "Authorization:{YOUR-API-KEY}" -d '{"name": "my-new-showcase"}'

    - show a showcase
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_show -d '{"id": "my-new-showcase"}'

    - list showcases
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_list -d ''


Dataset actions::

    - add a dataset to a showcase (sysadmins and showcase admins only)
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_package_association_create -H "Authorization:{YOUR-API-KEY}" -d '{"showcase_id": "my-showcase", "package_id": "my-package"}'

    - remove a dataset from a showcase (sysadmins and showcase admins only)
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_package_association_delete -H "Authorization:{YOUR-API-KEY}" -d '{"showcase_id": "my-showcase", "package_id": "my-package"}'

    - list datasets in a showcase
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_package_list -d '{"showcase_id": "my-showcase"}'

    - list showcases featuring a given dataset
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_package_showcase_list -d '{"package_id": "my-package"}'


Showcase admin actions::

    - add showcase admin (sysadmins only)
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_admin_add -H "Authorization:{YOUR-API-KEY}" -d '{"username": "bert"}'

    - remove showcase admin (sysadmins only)
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_admin_remove -H "Authorization:{YOUR-API-KEY}" -d '{"username": "bert"}'

    - list showcase admins (sysadmins only)
    curl -X POST http://127.0.0.1:5000/api/3/action/ckanext_showcase_admin_list -H "Authorization:{YOUR-API-KEY}" -d ''


---
UI
---

The Showcase extension adds the following pages to the user interface:


* The main showcase index is available on: ``http://127.0.0.1:5000/showcase``

* To create a new showcase: ``http://127.0.0.1:5000/showcase/new``

* To edit or delete a showcase: ``http://127.0.0.1:5000/showcase/edit/{showcase-name}``

* To add a Showcase Admin : ``http://127.0.0.1:5000/ckan-admin/showcase_admins``


---------------------
Configuration
---------------------

If you want to use the WYSIWYG editor instead of Markdown to write the content of the showcase::

    ckanext.showcase.editor = ckeditor

-----------------------------------------------
Migrating Showcases Notes from Markdown to HTML
-----------------------------------------------

When using CKEditor as WYSIWYG editor showcases notes are stored in HTML
instead of Markdown. To migrate all existing notes from markdown to
HTML you can use the ```showcase markdown_to_html``` command.

From the ``ckanext-showcase`` directory::

    ckan -c {path to production.ini} showcase markdown-to-html

-----------------
Running the Tests
-----------------

To run the tests, do::

    pytest --ckan-ini=test.ini ckanext/showcase/tests


------------------------------------
Registering ckanext-showcase on PyPI
------------------------------------

ckanext-showcase should be availabe on PyPI as
https://pypi.python.org/pypi/ckanext-showcase. If that link doesn't work, then
you can register the project on PyPI for the first time by following these
steps:

1. Create a source distribution of the project::

     python setup.py sdist

2. Register the project::

     python setup.py register

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the first release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.1 then do::

       git tag 0.0.1
       git push --tags


-------------------------------------------
Releasing a New Version of ckanext-showcase
-------------------------------------------

ckanext-showcase is availabe on PyPI as https://pypi.python.org/pypi/ckanext-showcase.
To publish a new version to PyPI follow these steps:

1. Update the version number in the ``setup.py`` file.
   See `PEP 440 <http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers>`_
   for how to choose version numbers.

2. Create a source distribution of the new version::

     python setup.py sdist

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the new release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.2 then do::

       git tag 0.0.2
       git push --tags


-------------------------------------------
i18n
-------------------------------------------

See: "Internationalizing strings in extensions" : http://docs.ckan.org/en/latest/extensions/translating-extensions.html

1. Install babel

       pip install Babel

2. Init Catalog for your language

       python setup.py init_catalog -l es

3. Compile your language catalog ( You can force pybabel compile to compile messages marked as fuzzy with the -f)

       python setup.py compile_catalog -f -l es

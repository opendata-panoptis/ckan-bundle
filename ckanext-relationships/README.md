# ckanext-relationships

This extension provides functional changes to the way relationships in CKAN work.

Core CKAN restrictions for creating relationships:

- Must always contain a subject package and object package
- User must have `package_update` permission on both subject and object packages

## External Relationships

In some CKAN instances it may be a requirement to create relationships to external URIs.

This extension enables this by allowing the `object_package_id` to be empty and
uses the `comment` to store the external URI.

## Relax package_relationship_create permissions

In some CKAN instances it may be a requirement to allow users to create relationships
from a package they DO have `package_update` permission for TO a package they DON'T
have `package_update` permission for, i.e. creating a relationship between a package
in an org they are an admin/editor in, to a package in another org that they are not
and admin/editor in.

### Config

This extension checks for the following setting in the CKAN `.ini` file:

    ckanext.relationships.package_relationship_create_ignore_auth_for

This should be set to a space separated list of user roles to allow, e.g.

    ckanext.relationships.package_relationship_create_ignore_auth_for = admin editor

## Φιλτράρισμα διαθέσιμων datasets για διασύνδεση

Από προεπιλογή, το dropdown δημιουργίας διασύνδεσης εμφανίζει μόνο datasets
που ανήκουν στον ίδιο οργανισμό με το τρέχον dataset. Για να επιτρέπονται
διασυνδέσεις με οποιοδήποτε dataset του portal, απενεργοποιήστε τη ρύθμιση:

    ckanext.relationships.relatable_datasets_same_org_only = false

Από προεπιλογή, η λίστα των διαθέσιμων datasets φορτώνεται απευθείας από τη
βάση αντί από το `package_search`, ώστε να διαβάζονται μόνο τα απαραίτητα
πεδία. Η επιλογή αυτή δεν επιστρέφει private ή deleted datasets και διαβάζει το
`title_translated` από το `package_extra`. Για να επιστρέψετε στη φόρτωση μέσω
`package_search`, απενεργοποιήστε τη ρύθμιση:

    ckanext.relationships.relatable_datasets_use_db_query = false

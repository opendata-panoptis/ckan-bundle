# ckanext-athens-theme

CKAN theme extension για την Πύλη Ανοικτών Δεδομένων του Δήμου Αθηναίων.

## Εγκατάσταση


Προσθήκη του `athens_theme` στη λίστα των plugins στο αρχείο `/etc/ckan/default/ckan.ini` πριν απο data-gov-gr:
```ini
ckan.plugins = ... athens_theme ...
```


## Διαμόρφωση

Το extension υποστηρίζει τις ακόλουθες παραμέτρους στο ckan.ini:

```ini
# Athens Theme Settings
ckan.site_title = Opendata.cityofathens
ckan.site_description = Πύλη Ανοικτών Δεδομένων Δήμου Αθηναίων

# Robots.txt
# Default: true. Ενεργοποιεί το εκτεταμένο robots.txt με αποκλεισμό
# AI/LLM crawlers, scrapers και CKAN faceted-search traps.
# Αν οριστεί σε false, χρησιμοποιείται το απλό CKAN-style robots.txt.
ckanext.athens_theme.robots.extended = true
```


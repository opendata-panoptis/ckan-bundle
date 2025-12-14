function isDataServiceSearchPage() {
    const path = window.location.pathname || '';
    return path.includes('/data-service');
}

function getLicenseSolrField() {
    return isDataServiceSearchPage() ? 'license' : 'res_license';
}

document.addEventListener('DOMContentLoaded', function() {
    // Αρχικοποίηση: εμφάνιση μόνο της απλής αναζήτησης στην αρχή
    const simpleTab = document.getElementById('simple-tab');
    const advancedTab = document.getElementById('advanced-tab');
    const simpleSearch = document.getElementById('simple_search');
    const advancedSearch = document.getElementById('advanced_search');
    console.log('Αρχικοποίηση');

    // Ανάλογα με την επιλογή του χρήστη, εμφανίζουμε τη σωστή καρτέλα
    simpleTab.addEventListener('click', function() {
        console.log('καρτέλα');

        event.preventDefault();

        const searchUrl = '/dataset';
        // window.location.href = searchUrl;

        simpleSearch.style.display = 'block';
        advancedTab.classList.remove('active');
        simpleTab.classList.add('active');
        advancedSearch.style.display = 'none';
    });

    advancedTab.addEventListener('click', function() {
        console.log('καρτέλα');

        event.preventDefault();

        simpleSearch.style.display = 'none';
        simpleTab.classList.remove('active');
        advancedTab.classList.add('active');
        advancedSearch.style.display = 'block';

        findFilters();
    });
});

document.addEventListener('DOMContentLoaded', function() {
    // Λίστα με τα ids των select που θέλουμε να εξαιρέσουμε από το αυτόματο submit φόρμας
    const excludedSelects = [
        'field-dcat_type',
        'field-theme',
        'field-hvd_category',
        'field-license'
    ];

    // Για κάθε select που θέλουμε να εξαιρέσουμε
    excludedSelects.forEach(function(selectId) {
        const select = document.getElementById(selectId);
        if (select) {
            // Προσθέτουμε event listener που σταματάει το event από το να φτάσει
            // στον handler του select-switch module
            select.addEventListener('change', function(e) {
                e.stopPropagation();
            }, true); // Το true κάνει το listener να εκτελεστεί στη φάση capturing
        }
    });
});

document.addEventListener('DOMContentLoaded', function() {
    // Αναζήτηση μέσα στις άδειες
    const licenseSearchInput = document.getElementById('license-filter-input');
    const licenseSelect = document.getElementById('field-license');

    if (licenseSearchInput && licenseSelect) {
        const licenseOptions = Array.from(licenseSelect.options);

        licenseSearchInput.addEventListener('input', function() {
            const term = licenseSearchInput.value.trim().toLowerCase();

            licenseOptions.forEach(function(option) {
                const text = option.textContent.toLowerCase();
                const matches = !term || text.includes(term);
                option.hidden = !matches;
            });
        });
    }

    // Ενημέρωση σύνοψης επιλεγμένων φίλτρων στα headers
    const filtersContainer = document.getElementById('filters');
    if (filtersContainer) {
        filtersContainer.addEventListener('change', function(event) {
            const target = event.target;
            if (target && target.tagName === 'SELECT' && target.multiple) {
                updateFilterSummaries();
            }
        });

        // Αρχικοποίηση περιλήψεων κατά την φόρτωση
        updateFilterSummaries();
    }
});

// Τα dropdowns πλέον γεμίζουν δυναμικά με τα δεδομένα από τα λεξιλόγια
// Η λογική φόρτωσης έχει μεταφερθεί στο template snippets/filters.html

document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('searchButton').addEventListener('click', function(event) {
        event.preventDefault(); // Αποτρέπει την ανανέωση της σελίδας

        // === BΗΜΑ 1: Διαβάζουμε ΟΛΑ τα υπάρχοντα φίλτρα από το URL ===
        const urlParams = new URLSearchParams(window.location.search);

        // Λήψη των επιλεγμένων τιμών από τα dropdowns της σύνθετης αναζήτησης
        const categorySelect = document.getElementById('field-theme');
        const typeSelect = document.getElementById('field-dcat_type');
        const hvdcategorySelect = document.getElementById('field-hvd_category');
        const licenseSelect = document.getElementById('field-license');
        const additionalInput = document.getElementById('additionalInput'); // Το input πεδίο

        const selectedCategories = Array.from(categorySelect.selectedOptions).map(option => option.value);
        const selectedTypes = typeSelect ? Array.from(typeSelect.selectedOptions).map(option => option.value) : [];
        const selectedHvdCategories = Array.from(hvdcategorySelect.selectedOptions).map(option => option.value);
        const selectedLicenses = Array.from(licenseSelect.selectedOptions).map(option => option.value);
        const additionalValue = additionalInput.value.trim();
        const isDataServiceSearch = isDataServiceSearchPage();
        const licenseSolrField = getLicenseSolrField();


        // === ΒΗΜΑ 2: Φτιάχνουμε την παράμετρο 'q' όπως και πριν ===
        let query = '';

        // ΒΟΗΘΗΤΙΚΗ ΣΥΝΑΡΤΗΣΗ ΓΙΑ ESCAPING
        function escapeSolrQueryValue(value) {
            // Κάνει escape τους ειδικούς χαρακτήρες του Solr
            return value.replace(/([\\+\-&|!(){}\[\]^"~*?:/])/g, "\\$1");
        }

        function getLastUriPart(uri) {
            if (!uri) return '';
            const parts = uri.split('/');
            return parts[parts.length - 1];  // Παίρνει το τελευταίο μέρος του URI
        }

        if (selectedHvdCategories.length > 0) {
            if (query) query += ' AND ';
            const categoryValues = selectedHvdCategories.map(value => `hvd_category:*${getLastUriPart(value)}*`);
            query += categoryValues.join(' AND ');
        }

        // Δημιουργία φίλτρου για τα themes (theme)
        if (selectedCategories.length > 0) {
            if (query) query += ' AND ';
            const themeValues = selectedCategories.map(value => `theme:*${getLastUriPart(value)}*`);
            query += themeValues.join(' AND ');
        }

        // Δημιουργία φίλτρου για τον τύπο (type)
        if (selectedTypes.length > 0) {
            if (query) query += ' AND ';
            const statusValues = selectedTypes.map(value => `dcat_type:*${getLastUriPart(value)}*`);
            query += statusValues.join(' AND ');
        }

        // Δημιουργία φίλτρου για τις άδειες (license)
        if (selectedLicenses.length > 0) {
            const licenseTerms = selectedLicenses
                .map(value => {
                    // Παίρνουμε μόνο το τελευταίο κομμάτι του URL (το ID της άδειας)
                    const licenseId = getLastUriPart(value);
                    if (!licenseId) {
                        return null;
                    }

                    const escapedId = escapeSolrQueryValue(licenseId);
                    if (!escapedId) {
                        return null;
                    }

                    if (isDataServiceSearch) {
                        // Τα data services δεν έχουν resources, οπότε κάνουμε match στο dataset-level πεδίο
                        return `${licenseSolrField}:*${escapedId}*`;
                    }

                    // Για τα datasets διατηρούμε το pattern με την απόστροφο ώστε να ταιριάζει στο resource-level query
                    return `${licenseSolrField}:*${escapedId}'*`;
                })
                .filter(Boolean);

            if (licenseTerms.length > 0) {
                if (query) query += ' AND ';

                if (licenseTerms.length > 1) {
                    query += `(${licenseTerms.join(' AND ')})`;
                } else {
                    query += licenseTerms[0];
                }
            }
        }

        // Δημιουργία φίλτρου για το επιπλέον input (additionalInput)
        if (additionalValue) {
            if (query) query += ' AND ';
            query += additionalValue;
        }

        // === BΗΜΑ 3: Ενώνουμε τα παλιά φίλτρα με το νέο 'q' ===

        // Αν ο χρήστης έβαλε κάτι στα πεδία της σύνθετης αναζήτησης,
        // θέτουμε την παράμετρο 'q' στο αντικείμενο urlParams.
        // Αυτό θα αντικαταστήσει τυχόν παλιό 'q' ή θα προσθέσει ένα νέο.
        if (query) {
            urlParams.set('q', query);
        } else {
            // Αν ο χρήστης άδειασε τα πεδία της σύνθετης αναζήτησης,
            // αφαιρούμε την παράμετρο 'q' από το URL.
            urlParams.delete('q');
        }

        // === BΗΜΑ 4: Δημιουργούμε το τελικό URL ===
        // Παίρνουμε το βασικό path της σελίδας (π.χ. /dataset/)
        const baseUrl = window.location.pathname;
        // Μετατρέπουμε το αντικείμενο urlParams ξανά σε string (π.χ. "formats=TXT&q=theme:*ECONOMY*")
        const newQueryString = urlParams.toString();
        // Ενώνουμε τα πάντα για το τελικό URL
        const fullUrl = `${baseUrl}?${newQueryString}`;

        // Ανακατεύθυνση στο νέο, συνδυαστικό URL
        window.location.href = fullUrl;
        console.log('Final combined URL:', fullUrl);
    });
});

// Αντίστροφη διαδικασία για εμφάνιση των επιλεγμένων φίλτρων
document.addEventListener('DOMContentLoaded', function() {
    findFilters();
});

function findFilters() {
    console.log('findFilters');

    // Αναλύουμε το query string από το URL
    const urlParams = new URLSearchParams(window.location.search);
    const query = urlParams.get('q');  // Παίρνουμε το query string μετά το 'q='
    const isDataServiceSearch = isDataServiceSearchPage();
    const licenseSolrField = getLicenseSolrField();

    // Αν το query string υπάρχει
    if (query) {
        // Αποκωδικοποιούμε το query string (αν περιέχει ειδικούς χαρακτήρες όπως %20 για κενό)
        const decodedQuery = decodeURIComponent(query);

        // Αναλύουμε το query string σε tokens (χωρίζουμε σε παρενθέσεις ή διαστήματα)
        const queryParams = decodedQuery.match(/(\([^)]+\)|[^( )]+)/g) || [];

        const freeTextTokens = [];
        let index = 0;

        // Διατρέχουμε τα tokens από την αρχή και "τρώνε" τα κομμάτια
        // που αντιστοιχούν στα γνωστά φίλτρα (Κατηγορίες, HVD, Τύποι, Άδειες).
        // Μόλις βρούμε κάτι που δεν αναγνωρίζουμε, θεωρούμε ότι από εκεί και πέρα
        // είναι το ελεύθερο κείμενο (Επιπλέον φίλτρο).
        while (index < queryParams.length) {
            const param = queryParams[index];
            const normalizedParam = param.replace(/^[()]+/, '').replace(/[()]+$/, '');

            // hvd_category
            if (normalizedParam.startsWith('hvd_category:*')) {
                const value = normalizedParam.split(':')[1].replaceAll('*', '');
                const baseUri = 'http://data.europa.eu/bna/';
                const fullValue = baseUri + value;
                const categorySelect = document.getElementById('field-hvd_category');
                const option = categorySelect
                    ? Array.from(categorySelect.options).find(o => o.value === fullValue)
                    : null;
                if (option) {
                    option.selected = true;
                }
                index += 1;
                continue;
            }

            // theme
            if (normalizedParam.startsWith('theme:*')) {
                const value = normalizedParam.split(':')[1].replaceAll('*', '');
                const baseUri = 'http://publications.europa.eu/resource/authority/data-theme/';
                const fullValue = baseUri + value;
                const themeSelect = document.getElementById('field-theme');
                const option = themeSelect
                    ? Array.from(themeSelect.options).find(o => o.value === fullValue)
                    : null;
                if (option) {
                    option.selected = true;
                }
                index += 1;
                continue;
            }

            // dcat_type
            if (normalizedParam.startsWith('dcat_type:*')) {
                const value = normalizedParam.split(':')[1].replaceAll('*', '');
                const baseUri = 'http://publications.europa.eu/resource/authority/dataset-type/';
                const fullValue = baseUri + value;
                const dcatSelect = document.getElementById('field-dcat_type');
                const option = dcatSelect
                    ? Array.from(dcatSelect.options).find(o => o.value === fullValue)
                    : null;
                if (option) {
                    option.selected = true;
                }
                index += 1;
                continue;
            }

            // Άδειες (dataset ή resource level)
            if (normalizedParam.startsWith(`${licenseSolrField}:`)) {
                const pattern = isDataServiceSearch ? /\*([^*]+)\*/ : /\*([^\*']+)'/;
                const match = normalizedParam.match(pattern);

                if (match && match[1]) {
                    const licenseId = match[1];
                    const licenseSelect = document.getElementById('field-license');
                    if (licenseSelect) {
                        const option = Array.from(licenseSelect.options).find(opt =>
                            opt.value.endsWith(licenseId)
                        );
                        if (option) {
                            option.selected = true;
                        }
                    }
                }
                index += 1;
                continue;
            }

            // Συνδετικές λέξεις ανάμεσα στα φίλτρα
            if (normalizedParam === 'AND' || normalizedParam === 'OR') {
                index += 1;
                continue;
            }

            // Οτιδήποτε άλλο θεωρείται αρχή του "Επιπλέον φίλτρου"
            freeTextTokens.push(...queryParams.slice(index));
            break;
        }

        // Συμπληρώνουμε το πεδίο ελεύθερου κειμένου με ό,τι βρήκαμε
        const additionalInput = document.getElementById('additionalInput');
        if (additionalInput) {
            if (freeTextTokens.length > 0) {
                additionalInput.value = freeTextTokens.join(' ');
            } else {
                additionalInput.value = '';
            }
        }
    }

    // Αφού ρυθμιστούν οι επιλογές από το URL, ενημερώνουμε τις σύνοψεις
    updateFilterSummaries();
}

function updateFilterSummaries() {
    const filtersContainer = document.getElementById('filters');
    if (!filtersContainer) {
        return;
    }

    const selects = filtersContainer.querySelectorAll('select[multiple]');
    selects.forEach(function(select) {
        const fieldName = select.name;
        const summaryElement = document.querySelector(
            '.filters-summary[data-summary-for="' + fieldName + '"]'
        );

        if (!summaryElement) {
            return;
        }

        const selectedOptions = Array.from(select.selectedOptions);
        if (!selectedOptions.length) {
            summaryElement.textContent = '';
            summaryElement.classList.remove('has-selection');
            return;
        }

        if (selectedOptions.length === 1) {
            summaryElement.textContent = selectedOptions[0].textContent;
        } else if (selectedOptions.length === 2) {
            summaryElement.textContent = selectedOptions[0].textContent +
                ', ' + selectedOptions[1].textContent;
        } else {
            const firstLabel = selectedOptions[0].textContent;
            const secondLabel = selectedOptions[1].textContent;
            const remainingCount = selectedOptions.length - 2;
            summaryElement.textContent = firstLabel + ', ' + secondLabel +
                ' +' + remainingCount;
        }

        summaryElement.classList.add('has-selection');
    });
}

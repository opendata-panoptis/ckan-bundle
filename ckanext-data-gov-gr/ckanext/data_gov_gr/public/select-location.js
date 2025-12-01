let index;
let selectedRow = null;
let geonamesList = [];

// Άνοιγμα dialog
function onOpenDialog(event, idx) {
  index = idx;
  console.log(index);
  suppressEvent(event);
  clearTableRows();
  document.body.style.overflow = 'hidden';
  document.getElementById('dialogOverlay').style.display = 'flex';

  searchGeonames("Athens").then(result => {
    loadGeonames(result.result.geonames);
  });
}

// Κλείσιμο dialog
function onCloseDialog(event) {
  suppressEvent(event);
  clearTableRows();
  document.body.style.overflow = '';
  document.getElementById('dialogOverlay').style.display = 'none';

  this.dispatchEvent(new CustomEvent('location-selected', {
    detail: { count: 5 },
    bubbles: true,
    composed: true
  }));
  clearSelection();
}

// Καθαρισμός Πεδίων
function clearSpecialCoverageFields(event, idx) {
    uriId = `field-spatial_coverage-${index}-uri`;
    uriValue = null;
    document.getElementById(uriId).value = uriValue;

    textId = `field-spatial_coverage-${index}-text`;
    textValue = null;
    document.getElementById(textId).value = textValue;

    geomId = `field-spatial_coverage-${index}-geom`;
    geomValue = null;
    document.getElementById(geomId).value = geomValue;

    bboxId = `field-spatial_coverage-${index}-bbox`;
    bboxValue = null;
    document.getElementById(bboxId).value = bboxValue;

    centroidId = `field-spatial_coverage-${index}-centroid`;
    centroidValue = null;
    document.getElementById(centroidId).value = centroidValue;

}

/**
 * Searches for geonames by query string.
 * @param {string} [query="Athens"]
 * @returns {Promise<Object|null>}
 */
async function searchGeonames(query = "Athens") {
  const endpoint = "/api/3/action/geonames_search";
  const csrfValue = document.querySelector('meta[name="_csrf_token"]')?.getAttribute('content');

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrfValue
      },
      body: JSON.stringify({ query })
    });

    if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);
    return await response.json();
  } catch (error) {
    console.error("Error during API request:", error);
    return null;
  }
}


function onSearch(event) {
  suppressEvent(event);
  const query = document.getElementById("searchInput").value.trim();
  clearSelection();

  if (!query) {
    alert("Παρακαλώ εισάγετε ένα όρο αναζήτησης.");
    return;
  }

  const tableBody = document.querySelector("#dataTable tbody");
  tableBody.innerHTML = "<tr><td colspan='6'> Αναζήτηση... </td></tr>";

  searchGeonames(query).then(result => {
    tableBody.innerHTML = "";
    let geonames = result.result.geonames;
    if (geonames && geonames.length > 0) {
      loadGeonames(geonames);
    } else {
      tableBody.innerHTML = "<tr><td colspan='6'> {{ _('No results found.') }}</td></tr>";
    }
  });
}

/**
 * Φορτώνει μια λίστα αντικειμένων τοποθεσιών (geonames) σε έναν HTML πίνακα.
 * Καθαρίζει τις υπάρχουσες γραμμές του πίνακα και στη συνέχεια γεμίζει τον πίνακα με τα δεδομένα.
 * Κάθε γραμμή γίνεται κλικαριστή και καλεί τη συνάρτηση `selectRow(row)` όταν πατηθεί.
 *
 * @param {Array<Object>} geonames - Πίνακας με αντικείμενα τοποθεσιών geonames.
 * @param {string} geonames[].name - Το όνομα της τοποθεσίας.
 * @param {string} geonames[].countryName - Το όνομα της χώρας.
 * @param {string} geonames[].adminName1 - Η διοικητική περιοχή (π.χ. νομός ή περιφέρεια).
 * @param {number} geonames[].population - Ο πληθυσμός της τοποθεσίας.
 * @param {number|string} geonames[].lat - Το γεωγραφικό πλάτος της τοποθεσίας.
 * @param {number|string} geonames[].lng - Το γεωγραφικό μήκος της τοποθεσίας.
 */
function loadGeonames(geonames) {
  clearTableRows();
  const tableBody = document.getElementById("dataBody");
  geonamesList = geonames;

  geonames.forEach(location => {
    const row = tableBody.insertRow();
    row.innerHTML = `
      <td>${location.name}</td>
      <td>${location.countryName}</td>
      <td>${location.adminName1}</td>
      <td>${location.population.toLocaleString()}</td>
      <td>${location.lat}</td>
      <td>${location.lng}</td>
    `;
    row.onclick = () => selectRow(row);
  });
}


/**
 * Επιλογή μιας γραμμής
 * @param {HTMLTableRowElement} row
 * @returns {void}
 */
function selectRow(row) {
  if (selectedRow) selectedRow.classList.remove('selected');
  selectedRow = row;
  row.classList.add('selected');
}

/**
 * Καθαρισμός Επιλογής
 * @returns {void}
 */

function clearSelection() {
  if (selectedRow) {
    selectedRow.classList.remove('selected');
    selectedRow = null;
  }
}

/**
 * Οτάν εκτελείται η επιβεβαίωση
 * @returns {void}
 */
function onConfirmSelection(event) {
  suppressEvent(event);

  if (selectedRow) {
    const name = selectedRow.getElementsByTagName('td')[0].innerText;
    const geoname = getGeonameIdByName(name);

    uriId = `field-spatial_coverage-${index}-uri`;
    uriValue = composeGeonamesUri(geoname.geonameId);
    document.getElementById(uriId).value = uriValue;

    textId = `field-spatial_coverage-${index}-text`;
    textValue = name;
    document.getElementById(textId).value = textValue;

    geomId = `field-spatial_coverage-${index}-geom`;
    geomValue = composeGeometry(geoname.lng, geoname.lat);
    document.getElementById(geomId).value = geomValue;

    centroidId = `field-spatial_coverage-${index}-centroid`;
    centroidValue = composeGeometry(geoname.lng, geoname.lat);
    document.getElementById(centroidId).value = centroidValue;

    onCloseDialog();
  } else {
    alert("Παρακαλώ επιλέξτε μια σειρά πρώτα.");
  }
}

/**
 * Καθαρισμός γραμμών
 * @returns {void}
 */
function clearTableRows() {
  const dataBody = document.getElementById('dataBody');
  if (dataBody) dataBody.innerHTML = '';
}

/**
 * Ανάκτηση geoname ID με βάση ονομα τοπονύμιου
 * @param {string} name
 * @returns {number|null}
 */
function getGeonameIdByName(name) {
  const match = geonamesList.find(item => item.name === name);
  console.log(match)
  return match ? match : null;
}

/**
 * Συνθέτει ένα URI GeoNames για ένα δεδομένο αναγνωριστικό GeoNames ID.
 *
 * @param {number | string}
 * @returns {string} The complete GeoNames URI.
 */
function composeGeonamesUri(geoNameId) {
  return `http://sws.geonames.org/${geoNameId}/`
}

function composeGeometry(lng, lat) {
   const geometry = {
    type: "Point",
    coordinates: [lng, lat]
  };
  return JSON.stringify(geometry);
}

function composeCentroid(lng, lat) {
   const centroid = [lng, lat];
  return JSON.stringify(centroid);
}
/**
 * Prevents default browser behavior and event bubbling.
 * @param {Event} event
 * @returns {void}
 */
function suppressEvent(event) {
  if (event) {
    event.stopPropagation();
    event.preventDefault();
  }
}

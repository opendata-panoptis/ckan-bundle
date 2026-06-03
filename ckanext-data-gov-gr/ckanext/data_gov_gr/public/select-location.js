let index;
let selectedRow = null;
let geonamesById = new Map();

// Άνοιγμα dialog
function onOpenDialog(event, idx) {
  index = idx;
  console.log(index);
  suppressEvent(event);
  clearTableRows();
  document.body.style.overflow = 'hidden';
  document.getElementById('dialogOverlay').style.display = 'flex';

  searchGeonames("Athens").then(result => {
    const geonames = result?.result?.geonames || [];
    loadGeonames(geonames);
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
    suppressEvent(event);
    const parsedIndex = Number.parseInt(idx, 10);
    const targetIndex = Number.isNaN(parsedIndex) ? index : parsedIndex;
    if (targetIndex === undefined || targetIndex === null) {
      return;
    }

    const uriInput = document.getElementById(`field-spatial_coverage-${targetIndex}-uri`);
    const textInput = document.getElementById(`field-spatial_coverage-${targetIndex}-text`);
    const geomInput = document.getElementById(`field-spatial_coverage-${targetIndex}-geom`);
    const bboxInput = document.getElementById(`field-spatial_coverage-${targetIndex}-bbox`);
    const centroidInput = document.getElementById(`field-spatial_coverage-${targetIndex}-centroid`);
    const personInput = document.getElementById(`personInput-${targetIndex}`);

    if (uriInput) uriInput.value = "";
    if (textInput) textInput.value = "";
    if (geomInput) geomInput.value = "";
    if (bboxInput) bboxInput.value = "";
    if (centroidInput) centroidInput.value = "";
    if (personInput) personInput.value = "";
}

/**
 * Fetch detailed GeoNames info for a specific geonameId.
 * @param {number|string} geonameId
 * @returns {Promise<Object|null>}
 */
async function getGeonameDetails(geonameId) {
  const endpoint = "/api/3/action/geonames_get";
  const csrfValue = document.querySelector('meta[name="_csrf_token"]')?.getAttribute('content');

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrfValue
      },
      body: JSON.stringify({ geonameId })
    });

    if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);
    return await response.json();
  } catch (error) {
    console.error("Error during GeoNames details API request:", error);
    return null;
  }
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
    const geonames = result?.result?.geonames || [];
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
  geonamesById = new Map();

  geonames.forEach(location => {
    const geonameId = String(location.geonameId || "");
    const population = Number(location.population);
    const populationDisplay = Number.isFinite(population) ? population.toLocaleString() : "";
    if (geonameId) {
      geonamesById.set(geonameId, location);
    }

    const row = tableBody.insertRow();
    row.dataset.geonameId = geonameId;
    row.innerHTML = `
      <td>${location.name}</td>
      <td>${location.countryName}</td>
      <td>${location.adminName1}</td>
      <td>${populationDisplay}</td>
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
async function onConfirmSelection(event) {
  suppressEvent(event);

  if (!selectedRow) {
    alert("Παρακαλώ επιλέξτε μια σειρά πρώτα.");
    return;
  }

  const geonameId = selectedRow.dataset.geonameId;
  const geoname = geonameId ? geonamesById.get(geonameId) : null;
  if (!geoname) {
    alert("Δεν ήταν δυνατή η ανάκτηση στοιχείων τοποθεσίας.");
    return;
  }

  const name = selectedRow.getElementsByTagName('td')[0].innerText;
  const detailsResult = await getGeonameDetails(geoname.geonameId);
  const bboxFromDetails = detailsResult?.result?.bbox || "";

  const uriInput = document.getElementById(`field-spatial_coverage-${index}-uri`);
  const textInput = document.getElementById(`field-spatial_coverage-${index}-text`);
  const geomInput = document.getElementById(`field-spatial_coverage-${index}-geom`);
  const bboxInput = document.getElementById(`field-spatial_coverage-${index}-bbox`);
  const centroidInput = document.getElementById(`field-spatial_coverage-${index}-centroid`);
  const personInput = document.getElementById(`personInput-${index}`);

  if (uriInput) uriInput.value = composeGeonamesUri(geoname.geonameId);
  if (textInput) textInput.value = name;
  if (geomInput) geomInput.value = composeGeometry(geoname.lng, geoname.lat);
  if (bboxInput) bboxInput.value = bboxFromDetails;
  if (centroidInput) centroidInput.value = composeGeometry(geoname.lng, geoname.lat);
  if (personInput) personInput.value = name;

  onCloseDialog(event);
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

document.addEventListener('DOMContentLoaded', function () {

  // Αν υπάρχουν αποθηκευμένα center/zoom από προηγούμενη αναζήτηση, τα χρησιμοποιούμε
  const savedCenter = localStorage.getItem('map_center');
  const savedZoom = localStorage.getItem('map_zoom');

  let map;

  if (savedCenter && savedZoom) {
    // Μετατροπή σε πίνακα αριθμών (lat, lng)
    const centerCoords = savedCenter.split(',').map(parseFloat);
    const zoomLevel = parseInt(savedZoom, 10);
    map = L.map('map').setView(centerCoords, zoomLevel);
  } else {
    // Αν δεν υπάρχουν αποθηκευμένα, χρησιμοποίησε προεπιλεγμένη προβολή (Ελλάδα)
    map = L.map('map').setView([38.0, 23.7], 6); // Προεπιλεγμένο (Ελλάδα)
  }

  // Προσθήκη βασικού επιπέδου (OpenStreetMap)
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors'
  }).addTo(map);

  // Συνάρτηση που υπολογίζει και ενημερώνει το κρυφό input ext_bbox με το τρέχον bounding box
  function updateBBoxInput() {
    const bounds = map.getBounds();
    const sw = bounds.getSouthWest();
    const ne = bounds.getNorthEast();
    const minx = sw.lng.toFixed(6);
    const miny = sw.lat.toFixed(6);
    const maxx = ne.lng.toFixed(6);
    const maxy = ne.lat.toFixed(6);
    const extBBoxValue = `${minx},${miny},${maxx},${maxy}`;

    document.getElementById('ext_bbox_input').value = extBBoxValue;
  }

  // Κάθε φορά που κινείται ο χάρτης, ενημέρωσε το bbox
  map.on('moveend', updateBBoxInput);
  updateBBoxInput();   // Ενημέρωση με την αρχική θέση

  const searchBtn = document.getElementById('map-search-button');
  if (searchBtn) {
    searchBtn.addEventListener('click', function (e) {
      e.preventDefault();

      updateBBoxInput();     // Βεβαιωνόμαστε ότι είναι το πιο πρόσφατο bbox

      // Αποθήκευση της τρέχουσας κατάστασης του χάρτη για μελλοντική χρήση
      const center = map.getCenter();
      const zoom = map.getZoom();
      localStorage.setItem('map_center', `${center.lat},${center.lng}`);
      localStorage.setItem('map_zoom', zoom);

      const form = document.getElementById('dataset-search-form');

      // Αν δεν υπάρχει ήδη το πεδίο ext_bbox στο form, προσθέτουμε νέο input
      let extInput = form.querySelector('input[name="ext_bbox"]');
      if (!extInput) {
        extInput = document.createElement('input');
        extInput.type = 'hidden';
        extInput.name = 'ext_bbox';
        form.appendChild(extInput);
      }

      // Ορισμός της τιμής bbox στο input
      extInput.value = document.getElementById('ext_bbox_input').value;

      // Υποβολή της φόρμας
      form.submit();
    });
  }
});

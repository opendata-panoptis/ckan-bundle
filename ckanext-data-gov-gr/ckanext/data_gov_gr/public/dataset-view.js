
  function translateGreekFieldToEnglish(fieldName, package_id) {


    const greekTextFieldId = `field-${fieldName}-el`;
    const englishTextFieldId = `field-${fieldName}-en`;

    greekText = document.getElementById(greekTextFieldId).value ;

    translate(greekText, package_id).then(result => {
      if (result) {
        // Εξαγωγή μεταφρασμενου από την απάντηση
        const english_text = result["result"][0]["translations"][0]["text"];
        const englishField = document.getElementById(englishTextFieldId);



        // Εμφάνιση του modal
        const modal = document.getElementById('confirmModal');

        modal.open(english_text, 'Επιβεβαίωση Μετάφρασης');

        modal.addEventListener('confirm', (e) => {
          if (e.detail === true) {

              // Ορίζουμε σαν προσωρινή τιμή το αγγλικό κείμενο
              englishField.value = english_text;

              // Ορίζουμε highlight στο πεδίο
              englishField.style.border = "2px solid #4CAF50";
              englishField.style.backgroundColor = "#e8f5e9";
          } else {
            console.log("User clicked NO");
            // cancel or ignore
          }
        });


        // Αφαίρεση Highlight
        setTimeout(() => {
          englishField.style.border = "";
          englishField.style.backgroundColor = "";
        }, 3000); // optional delay before removing highlight


      } else {
        console.log("Translation failed.");
      }
    }).catch(error => {
      console.error("Unexpected error:", error);
    });
  }

/**
 * Μεταφράζει το δοθέν κείμενο στα Αγγλικά χρησιμοποιώντας API μετάφρασης βασισμένο στο Azure.
 *
 * @async
 * @function translate
 * @param {string} text - Το κείμενο προς μετάφραση.
 * @param {string|number} package_id - Το αναγνωριστικό του πακέτου που σχετίζεται με το αίτημα μετάφρασης.
 * @returns {Promise<Object|null>} Τα μεταφρασμένα δεδομένα που επιστρέφονται από το API ή `null` σε περίπτωση σφάλματος.
 *
 * @throws {Error} Αν η HTTP απόκριση δεν είναι επιτυχής (OK).
 */
  async function translate(text, package_id) {

    const endpoint = "/api/action/azure_translate";
    const csrfValue = document.querySelector('meta[name="_csrf_token"]')?.getAttribute('content');

    try {
      const requestBody = {
        text: text,
        to_lang: "en",
        id: package_id
      };

      const response = await fetch(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": csrfValue
        },
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }

      const data = await response.json();
      return data;

    } catch (error) {
      console.error("Error during API request:", error);
      return null;
    }
  }

  /**
 * Εμφανίζει ένα παράθυρο επιβεβαίωσης με το παρεχόμενο κείμενο ερώτησης και επιστρέφει την απάντηση του χρήστη.
 *
 * @function showConfirmation
 * @param {string} questionText - Το μήνυμα ή η ερώτηση που θα εμφανιστεί στο παράθυρο επιβεβαίωσης.
 * @returns {Promise<boolean>} Ένα promise που επιλύεται σε `true` αν ο χρήστης επιβεβαιώσει, ή `false` αν ακυρώσει.
 *
 */
  function showConfirmation(questionText) {
    return new Promise((resolve) => {
      const modal = document.getElementById('confirmModal');

      const handleConfirm = (event) => {
        resolve(event.detail); // true (Yes) or false (No)
        modal.removeEventListener('confirm', handleConfirm);
      };

      modal.addEventListener('confirm', handleConfirm);
      modal.open(questionText);
    });
  }



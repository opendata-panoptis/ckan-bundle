/**
 * GovgrConfirmModal
 * Επαναχρησιμοποιήσιμο modal Web Component
 * Εμφανίζει ένα παράθυρο επιβεβαίωσης με παραμετροποιήσιμη ερώτηση και κουμπιά Ναι/Όχι.
 * Εκπέμπει ένα event 'confirm' με detail=true (Ναι) ή detail=false (Όχι).
 */
class GovgrConfirmModal extends HTMLElement {
  constructor() {
    super();
    // Επισυνάπτει Shadow DOM για απομόνωση στυλ και HTML
    this.attachShadow({ mode: 'open' });

    // Ορισμός HTML και CSS για το modal στο shadow root
    this.shadowRoot.innerHTML = `
      <style>
        /* Στυλ για το στοιχείο και μεταβλητές CSS */
        :host {
          font-family: 'Noto Sans', sans-serif;
          --primary-color: #0033A0;  /* Μπλε Gov.gr */
          --background-overlay: rgba(255, 255, 255, 0.5); /* light white overlay */
          --modal-bg: #fff;  /* λευκό φόντο modal */
          --text-color: #001B54; /* μπλε σκούρο κείμενο */
        }

        /* Κάλυμμα modal, αρχικά κρυφό */
        .modal {
          position: fixed;
          top: 0; left: 0; right: 0; bottom: 0;
          background: var(--background-overlay);
          display: flex;
          justify-content: center;
          align-items: center;
          z-index: 9999;
          visibility: hidden;
          opacity: 0;
          transition: opacity 0.3s ease;
        }

        /* Εμφάνιση modal όταν υπάρχει η κλάση 'show' */
        .modal.show {
          visibility: visible;
          opacity: 1;
        }

        /* Περιεχόμενο modal */
        .modal-content {
          background: var(--modal-bg);
          border-radius: 8px;
          width: 420px;
          padding: 1.75rem 2rem;
          box-shadow: 0 0 20px rgba(0, 32, 91, 0.2);
          color: var(--text-color);
          text-align: center;
        }

        /* Τίτλος modal */
        h2 {
          margin-top: 0;
          font-size: 1.5rem;
          border-bottom: 2px solid var(--primary-color);
          padding-bottom: 0.5rem;
          margin-bottom: 1.5rem;
        }

        /* Κουμπιά */
        .actions {
          display: flex;
          justify-content: center;
          gap: 1rem;
        }

        /* Κοινό στυλ κουμπιών */
        button {
          padding: 0.75rem 2rem;
          border-radius: 4px;
          font-weight: bold;
          cursor: pointer;
          border: none;
          font-size: 1rem;
          min-width: 100px;
          transition: background-color 0.2s ease;
        }

        /* Στυλ για το κουμπί Ναι */
        button.yes {
          background-color: var(--primary-color);
          color: white;
        }
        button.yes:hover {
          background-color: #002270;
        }

        /* Στυλ για το κουμπί Όχι */
        button.no {
          background-color: transparent;
          border: 2px solid var(--primary-color);
          color: var(--primary-color);
        }
        button.no:hover {
          background-color: #e0e7ff;
        }
      </style>

      <!-- HTML δομή modal -->
      <div class="modal" role="dialog" aria-modal="true" aria-labelledby="modalTitle" aria-describedby="modalDesc">
        <div class="modal-content">
          <h2 id="modalTitle">Επιβεβαίωση</h2>
          <!-- Ερώτηση (με δυνατότητα παραμετροποίησης) -->
          <p id="modalDesc">Είστε σίγουρος/η;</p>
          <div class="actions">
            <button class="yes" part="yes-button">Ναι</button>
            <button class="no" part="no-button">Όχι</button>
          </div>
        </div>
      </div>
    `;

    // Αποθήκευση αναφορών σε στοιχεία για χρήση σε γεγονότα
    this.modal = this.shadowRoot.querySelector('.modal');
    this.yesBtn = this.shadowRoot.querySelector('button.yes');
    this.noBtn = this.shadowRoot.querySelector('button.no');

    // Δεσμεύει τα event handlers για διατήρηση του "this"
    this.handleYes = this.handleYes.bind(this);
    this.handleNo = this.handleNo.bind(this);
  }

  /**
   * Καλείται όταν το στοιχείο προστίθεται στο DOM
   */
  connectedCallback() {
    // Ορίζουμε event listeners για τα κουμπιά
    this.yesBtn.addEventListener('click', this.handleYes);
    this.noBtn.addEventListener('click', this.handleNo);
  }

  /**
   * Καλείται όταν το στοιχείο αφαιρείται από το DOM
   */
  disconnectedCallback() {
    // Αφαιρούμε του event listeners από τα κουμπιά
    this.yesBtn.removeEventListener('click', this.handleYes);
    this.noBtn.removeEventListener('click', this.handleNo);
  }

  /**
   * Εμφανίζει το modal και (προαιρετικά) ορίζει το κείμενο της ερώτησης
   * @param {string} question - Το κείμενο της ερώτησης (προαιρετικό)
   */
  open(question, title) {
    if (question) {
      this.shadowRoot.getElementById('modalDesc').textContent = question;
    }
    if (title) {
      this.shadowRoot.getElementById('modalTitle').textContent = title;
    }
    this.modal.classList.add('show');
    this.yesBtn.focus();
  }

  /**
   * Κλείνει το modal
   */
  close() {
    this.modal.classList.remove('show');
  }

  /**
   * Χειριστής όταν ο χρήστης πατήσει το κουμπί Ναι
   * Εκπέμπει γεγονός 'confirm' με detail=true και κλείνει το modal
   */
  handleYes() {
    this.dispatchEvent(new CustomEvent('confirm', { detail: true }));
    this.close();
  }

  /**
   * Χειριστής όταν ο χρήστης πατήσει το κουμπί Όχι
   * Εκπέμπει γεγονός 'confirm' με detail=false και κλείνει το modal
   */
  handleNo() {
    this.dispatchEvent(new CustomEvent('confirm', { detail: false }));
    this.close();
  }
}

// Καταχώρηση του custom στοιχείου ως <govgr-confirm-modal>
customElements.define('govgr-confirm-modal', GovgrConfirmModal);

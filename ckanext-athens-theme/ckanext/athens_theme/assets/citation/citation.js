/* Αντιγράφει την αναφορά του dataset στο πρόχειρο.
 *
 * Το κουμπί αντιγραφής μένει κρυφό μέχρι να τρέξει το module, οπότε χωρίς
 * JavaScript η αναφορά εμφανίζεται ως απλό κείμενο που επιλέγεται.
 *
 * copiedText - μήνυμα μετά από επιτυχή αντιγραφή
 * failedText - μήνυμα όταν ο browser δεν επιτρέπει την αντιγραφή
 */
this.ckan.module('athens-citation', function ($) {
  return {
    options: {
      copiedText: 'Citation copied',
      failedText: 'Copy failed. Select the text and copy it manually.'
    },

    initialize: function () {
      $.proxyAll(this, /_on/);

      this.text = this.$('.athens-citation-text');
      this.status = this.$('.athens-citation-status');
      this.$('.athens-citation-copy').prop('hidden', false).on('click', this._onCopy);
    },

    _onCopy: function () {
      var self = this;
      // Το innerText δίνει το κείμενο όπως εμφανίζεται (white-space: pre-line)
      var text = this.text[0].innerText;

      // Το Clipboard API είναι διαθέσιμο μόνο σε HTTPS και localhost
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(
          function () { self._showStatus(self.options.copiedText); },
          function () { self._fallbackCopy(); }
        );
      } else {
        this._fallbackCopy();
      }
    },

    _fallbackCopy: function () {
      var selection = window.getSelection();
      var range = document.createRange();
      var copied = false;

      range.selectNodeContents(this.text[0]);
      selection.removeAllRanges();
      selection.addRange(range);

      try {
        copied = document.execCommand('copy');
      } catch (error) {}

      if (copied) {
        selection.removeAllRanges();
        this._showStatus(this.options.copiedText);
      } else {
        // Το κείμενο μένει επιλεγμένο για χειροκίνητη αντιγραφή
        this._showStatus(this.options.failedText);
      }
    },

    _showStatus: function (message) {
      var self = this;

      // Καθαρισμός πρώτα, ώστε οι screen readers να ανακοινώσουν ξανά και το ίδιο μήνυμα
      clearTimeout(this.statusTimer);
      this.status.text('');
      this.statusTimer = setTimeout(function () {
        self.status.text(message);
        self.statusTimer = setTimeout(function () { self.status.text(''); }, 5000);
      }, 100);
    }
  };
});

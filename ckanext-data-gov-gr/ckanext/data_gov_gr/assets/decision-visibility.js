this.ckan.module('decision-visibility', function ($) {
  return {
    initialize: function () {
      // Δεν χρειάζεται data attribute: η λογική sysadmin/non-sysadmin έγινε ήδη στο template
      var $select = this.el.find('#field-private');

      // Αν υπάρχει ήδη μόνο Private, δεν κάνουμε τίποτα
      if ($select.find('option').length === 1) return;

      // Αν για κάποιο λόγο εμφανίστηκε Public σε μη-sysadmin, αφαίρεσέ το και κλείδωσε private.
      var hasPublic = $select.find('option[value="False"]').length > 0;
      if (hasPublic && $('body').data('is-sysadmin') !== true) {
        $select.find('option[value!="True"]').remove();
        $select.val('True');
      }
    }
  };
});
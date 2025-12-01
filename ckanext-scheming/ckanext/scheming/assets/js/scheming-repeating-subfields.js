ckan.module('scheming-repeating-subfields', function($) {
  return {
    initialize: function() {
      $.proxyAll(this, /_on/);

      var $template = this.el.children('div[name="repeating-template"]');
      this.template = $template.html();
      $template.remove();
      // click=_onCreateGroup
      this.el.find('a[name="repeating-add"]').on("click", this._onCreateGroup);
      // click=_onRemoveGroup
      this.el.on('click', 'a[name="repeating-remove"]', this._onRemoveGroup);
    },

    /**
     * Add new group to the fieldset.
     *
     * Fields inside every new group must be renamed in order to form correct
     * structure during validation:
     *
     *  ...
     *  (parent, INDEX-1, child-1),
     *  (parent, INDEX-1, child-2),
     *  ---
     *  (parent, INDEX-2, child-1),
     *  (parent, INDEX-2, child-2),
     *  ...
     */
    _onCreateGroup: function(e) {
        // Βρίσκουμε την τελευταία ομάδα επαναλήψεων
        var $last = this.el.find('.scheming-subfield-group').last();

        // Παίρνουμε τον δείκτη της τελευταίας ομάδας και αυξάνουμε κατά 1, ή 0 αν δεν υπάρχει καμία
        var group = ($last.data('groupIndex') + 1) || 0;

        // Κλωνοποιούμε το HTML template αντικαθιστώντας τα REPEATING-INDEX0/1 με τους πραγματικούς αριθμούς
        var $copy = $(
          this.template.replace(/REPEATING-INDEX0/g, group)
                       .replace(/REPEATING-INDEX1/g, group + 1)
        );

        // Προσθέτουμε το κλωνοποιημένο HTML μέσα στο container που περιέχει τις ομάδες
        this.el.find('.scheming-repeating-subfields-group').append($copy);

        // Αρχικοποιούμε οποιαδήποτε CKAN module υπάρχει στη νέα ομάδα
        this.initializeModules($copy);

        // Εμφανίζουμε τη νέα ομάδα με απλή κινούμενη εμφάνιση
        $copy.hide().show(100);

        // Βάζουμε το focus στο πρώτο πεδίο input της νέας ομάδας
        $copy.find('input').first().focus();

        // Εκπέμπουμε ένα προσαρμοσμένο event που ενημερώνει ότι η ομάδα αρχικοποιήθηκε
        this.el.trigger('scheming.subfield-group-init');
        e.preventDefault();
    },

    /**
     * Remove existing group from the fieldset.
     */
    _onRemoveGroup: function(e) {
        debugger;
        var $curr = $(e.target).closest('.scheming-subfield-group');
        var $body = $curr.find('.panel-body.fields-content');
        var $button = $curr.find('.btn-repeating-remove');
        var $removed = $curr.find('.panel-body.fields-removed-notice');
        $button.hide();
        $removed.show(100);
        $body.hide(100, function() {
          $body.html('');
        });
        e.preventDefault();
    },

    /**
     * Enable functionality of data-module attribute inside dynamically added
     * groups.
     */
    initializeModules: function(tpl) {
      $('[data-module]', tpl).each(function (index, element) {
        ckan.module.initializeElement(this);
      });
    }
  };
});

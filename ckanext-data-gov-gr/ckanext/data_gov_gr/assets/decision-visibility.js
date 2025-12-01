this.ckan.module('decision-visibility', function ($) {
  return {
    initialize: function () {
      var $container = this.el;
      var $select = $container.find('#field-private');
      var allowOrgAdminsPublic = $container.data('allow-org-admins-public');
      var isSysadmin = $('body').data('is-sysadmin') === true;

      if ($select.find('option').length === 1) return;

      var hasPublic = $select.find('option[value="False"]').length > 0;

      if (hasPublic && !isSysadmin && !allowOrgAdminsPublic) {
        $select.find('option[value!="True"]').remove();
        $select.val('True');
      }
    }
  };
});
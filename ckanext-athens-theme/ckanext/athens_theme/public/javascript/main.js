/* Athens Theme Main JavaScript */
"use strict";

ckan.module('athens-theme', function ($) {
  return {
    initialize: function () {
      // Language selector functionality
      $('.lang-select .lang-link').on('click', function(e) {
        if ($(this).hasClass('active')) {
          e.preventDefault();
        }
      });

      // Responsive menu toggle
      $('.nav-toggle').on('click', function() {
        $('.nav-pills').toggleClass('nav-open');
      });

      // Search form enhancement
      $('.site-search').on('submit', function(e) {
        var searchInput = $('#field-sitewide-search');
        if (searchInput.val().trim() === '') {
          e.preventDefault();
          searchInput.focus();
        }
      });
    }
  };
});

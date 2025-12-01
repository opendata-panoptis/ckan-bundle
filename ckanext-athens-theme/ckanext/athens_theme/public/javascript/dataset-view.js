// Dataset view specific JavaScript functionality
"use strict";

// Add any dataset-specific JavaScript functionality here
console.log("Dataset view JavaScript loaded");

// Example functionality for dataset pages
document.addEventListener('DOMContentLoaded', function() {
  // Add any dataset-specific initialization code here
  console.log("Dataset page initialized");
  
  // Example: Add a click handler for resource download buttons
  const downloadButtons = document.querySelectorAll('.resource-url-analytics');
  downloadButtons.forEach(function(button) {
    button.addEventListener('click', function(event) {
      console.log('Resource download initiated');
      // Add any analytics tracking or other functionality here
    });
  });
  
  // Example: Enhance search functionality on dataset pages
  const searchForms = document.querySelectorAll('.search-form');
  searchForms.forEach(function(form) {
    form.addEventListener('submit', function(event) {
      const searchInput = form.querySelector('input[name="q"]');
      if (searchInput && searchInput.value.trim() === '') {
        event.preventDefault();
        searchInput.focus();
        return false;
      }
    });
  });
});
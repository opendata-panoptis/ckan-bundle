/* JavaScript for the vocabulary admin extension */

function vocabularyadminUpdateTagOrderInput($container) {
  const ids = [];

  $container.find('.vocabulary-tag').each(function () {
    const id = $(this).data('id');
    if (id) {
      ids.push(id);
    }
  });

  $('#tag_order').val(ids.join(','));
}

function vocabularyadminInitTagDragAndDrop($container) {
  if (!$container || !$container.length) {
    return;
  }

  const $tagContainers = $container.find('.vocabulary-tag-container');
  $tagContainers.attr('draggable', 'true');

  let dragSrcEl = null;

  function getDataTransfer(e) {
    return e && e.originalEvent && e.originalEvent.dataTransfer
      ? e.originalEvent.dataTransfer
      : null;
  }

  // dragstart
  $container.on('dragstart', '.vocabulary-tag-container', function (e) {
    dragSrcEl = this;
    const $this = $(this);
    $this.addClass('dragging');

    const dt = getDataTransfer(e);
    if (dt) {
      dt.effectAllowed = 'move';
      dt.setData('text/plain', '');
    }
  });

  // dragenter: highlight potential drop target
  $container.on('dragenter', '.vocabulary-tag-container', function (e) {
    if (e.preventDefault) {
      e.preventDefault();
    }
    if (this !== dragSrcEl) {
      $(this).addClass('drag-over');
    }
  });

  // dragover: allow drop
  $container.on('dragover', '.vocabulary-tag-container', function (e) {
    if (e.preventDefault) {
      e.preventDefault();
    }
    const dt = getDataTransfer(e);
    if (dt) {
      dt.dropEffect = 'move';
    }
    return false;
  });

  // dragleave: remove highlight
  $container.on('dragleave', '.vocabulary-tag-container', function () {
    $(this).removeClass('drag-over');
  });

  // drop
  $container.on('drop', '.vocabulary-tag-container', function (e) {
    if (e.stopPropagation) {
      e.stopPropagation();
    }

    $(this).removeClass('drag-over');

    if (dragSrcEl && dragSrcEl !== this) {
      const $drag = $(dragSrcEl);
      const $target = $(this);

      if ($drag.index() < $target.index()) {
        $target.after($drag);
      } else {
        $target.before($drag);
      }

      vocabularyadminUpdateTagOrderInput($container);
    }

    return false;
  });

  // dragend
  $container.on('dragend', '.vocabulary-tag-container', function () {
    $(this).removeClass('dragging');
    $container.find('.drag-over').removeClass('drag-over');
    dragSrcEl = null;
  });

  // initial sync
  vocabularyadminUpdateTagOrderInput($container);
}

// Wait for the DOM to be ready
$(document).ready(function() {
  // Toggle hidden tags when clicking on the "more tags" message
  $('.vocabulary-list').on('click', '.more-tags-message', function() {
    var vocabularyId = $(this).data('vocabulary-id');
    var hiddenTagsContainer = $('#hidden-tags-' + vocabularyId);
    var expandIcon = $(this).find('.expand-icon i');

    // Toggle the visibility of the hidden tags
    hiddenTagsContainer.slideToggle(300, function() {
      // Update the icon based on the current state
      if (hiddenTagsContainer.is(':visible')) {
        expandIcon.removeClass('fa-chevron-down').addClass('fa-chevron-up');
      } else {
        expandIcon.removeClass('fa-chevron-up').addClass('fa-chevron-down');
      }
    });
  });

  // Use event delegation for tooltips - more efficient for many elements
  // Attach the event to the parent container instead of each tag
  $('.vocabulary-list').on('mouseenter', '.vocabulary-tag', function() {
    // Initialize tooltip only when needed (when mouse enters the tag)
    $(this).tooltip({
      title: function() {
        var tooltipContent = '<strong>Tag ID:</strong> ' + $(this).data('id');

        // Add URI if available
        if ($(this).data('uri')) {
          tooltipContent += '<br><strong>URI:</strong> ' + $(this).data('uri');
        }

        // Add labels if available
        if ($(this).data('label-el')) {
          tooltipContent += '<br><strong>Ετικέτα (EL):</strong> ' + $(this).data('label-el');
        }
        if ($(this).data('label-en')) {
          tooltipContent += '<br><strong>Ετικέτα (EN):</strong> ' + $(this).data('label-en');
        }

        // Add descriptions if available
        if ($(this).data('desc-el')) {
          tooltipContent += '<br><strong>Περιγραφή (EL):</strong> ' + $(this).data('desc-el');
        }
        if ($(this).data('desc-en')) {
          tooltipContent += '<br><strong>Περιγραφή (EN):</strong> ' + $(this).data('desc-en');
        }

        return tooltipContent;
      },
      html: true,
      container: 'body'
    }).tooltip('show');
  });

  // Table row hover effect is now handled by CSS
  // .vocabulary-admin-page .vocabulary-list tbody tr:hover { background-color: #f9f9f9; cursor: pointer; }

  // Add confirmation dialog for all delete buttons
  $('a.btn-danger').on('click', function(e) {
    var confirmation = confirm('Είστε σίγουρος ότι θέλετε να προχωρήσετε με τη διαγραφή;');
    if (!confirmation) {
      e.preventDefault(); // Cancel the action if user clicks Cancel
    }
  });

  // Form validation
  $('form').on('submit', function(e) {
    var isValid = true;

    // Check required fields
    $(this).find('[required]').each(function() {
      if (!$(this).val()) {
        isValid = false;
        $(this).addClass('error');
        $(this).closest('.control-group').addClass('error');
      } else {
        $(this).removeClass('error');
        $(this).closest('.control-group').removeClass('error');
      }
    });

    if (!isValid) {
      e.preventDefault();
      alert('Παρακαλώ συμπληρώστε όλα τα υποχρεωτικά πεδία.');
    }
  });

  // Initialize drag & drop ordering on vocabulary edit page
  var $tagsContainer = $('.vocabulary-tags-section .vocabulary-tags');
  if ($tagsContainer.length && $('#tag_order').length) {
    vocabularyadminInitTagDragAndDrop($tagsContainer);
  }

});

/*
 * Modal contact form, triggered from the a tag the module is loaded onto. Uses ajax to post the
 * contact form to the server.
 *
 * Example:
 *
 *   <a data-module="modal-contact">Contact</a>
 *
 */
ckan.module('modal-contact', function ($, _) {
  let self;

  return {
    /**
     * Initialises the module by setting up the recaptcha if necessary and setting up event
     * listeners.
     */
    initialize: function () {
      self = this;
      self.modal = null;
      self.messages = {
        onSuccess:
          self.options.successMessage ||
          self.i18n(
            _(
              'Thank you for contacting us, and we will try and reply as soon ' +
                'as possible.<br />Unfortunately due to the number of enquiries ' +
                'received, we cannot always reply in person to every one.',
            ),
          ),
        onError:
          self.options.errorMessage ||
          self.i18n(
            _(
              'Sorry, there was an error sending the email. Please try again later.',
            ),
          ),
        closeLabel: self.options.closeLabel || self.i18n(_('Close')),
      };
      self.options.scrollOnFlash =
        self.options.scrollOnFlash === true ||
        self.options.scrollOnFlash === 'true' ||
        self.options.scrollOnFlash === 1 ||
        self.options.scrollOnFlash === '1';
      // define the template if it is not passed
      self.options.template = self.options.template || 'contact_form.html';
      self.el.on('click', self._onClick);
    },

    /**
     * Loads and displays the contact form modal.
     */
    show: function () {
      self.sandbox.client.getTemplate(
        'contact_form.html',
        self.options,
        function (html) {
          // initialise the recaptcha context. By doing this here in the show function we
          // avoid showing the recaptcha badge on the page before the user has even given an
          // indication that they want to contact us which avoids confusion
          self.context = window.contacts_recaptcha.load(
            self.options.key,
            self.options.action,
          );

          self.modal = $(html);

          // Add required attributes and visual indicators
          self.modal.find('input[name="name"]').attr('required', true);
          self.modal.find('input[name="email"]').attr('required', true);

          // --- Populate default form values from options ---
            // Populate default form values from options
            if (self.options.dataName && self.options.dataName !== true) {
                self.modal.find('input[name="name"]').val(self.options.dataName).prop('readonly', false);
            }
            if (self.options.dataEmail && self.options.dataEmail !== true) {
                self.modal.find('input[name="email"]').val(self.options.dataEmail).prop('readonly', false);
            }
            if (self.options.dataSubject) {
                self.modal.find('input[name="subject"]').val(self.options.dataSubject);
            }
            if (self.options.dataContent) {
                self.modal.find('textarea[name="content"]').val(self.options.dataContent);
            }
          // add a close button to the modal header
          console.log(self.options.packageId )
          self.modal
            .find('.modal-header :header');
          self.modal.find('form').append('<input type="hidden" name="package_id" value="' + self.options.packageId + '">');

          // hook onto the submit event of the form
          self.modal.find('form').submit(function (event) {
            event.preventDefault();

            let form = self.modal.find('form');
            if (self.context) {
              self.context.addToken(form).then(function (token) {
                self.sendForm(form);
              });
            } else {
              self.sendForm(form);
            }

            // TODO: Add cancel button
          });

          // initialize popovers if present
          self.modal.find('[data-bs-toggle="popover"]').each(function () {
            new bootstrap.Popover(this);
          });

          // append modal to body sandbox and show it
          self.modal.appendTo(self.sandbox.body);
          let modalInstance = new bootstrap.Modal(self.modal[0]);
          modalInstance.show();
        },
      );
},

    /**
     * Sends the form's data to the server.
     *
     * @param form the form element to harvest the data from
     */
    sendForm: function (form) {
      $.ajax({
        url: '/contact/ajax',
        type: 'POST',
        data: form.serialize(),
        success: function (results) {
          if (results.success) {
            // it worked, woo!
            self.hide();
            self.flash_success(self.messages.onSuccess);
          } else if (!$.isEmptyObject(results.errors)) {
            // there were errors in the inputs from the user, likely missing values
            self.processFormError(form, results.errors);
          } else if (!!results.recaptcha_error) {
            // the recaptcha failed
            self.hide();
            self.flash_error(results.recaptcha_error);
          } else {
            // if we get here then something went wrong server side, probably when
            // sending the email
            self.hide();
            self.flash_error(self.messages.onError);
          }
        },
      });
    },

    /**
     * Process errors returned from form submission process.
     */
    processFormError: function (form, errors) {
      // remove all errors & classes
      form.find('.error-block').remove();
      form.find('.error').removeClass('error');
      form.find('.is-invalid').removeClass('is-invalid');

      // loop through all the errors, adding the error message and error classes
      for (let k in errors) {
        if (!Object.prototype.hasOwnProperty.call(errors, k)) {
          continue;
        }

        const field = form.find("[name='" + k + "']").first();
        if (!field.length) {
          continue;
        }

        const rawMessage = errors[k];
        const message = Array.isArray(rawMessage)
          ? rawMessage.join(', ')
          : String(rawMessage || '');

        const group = field.closest('.form-group, .control-group');
        const target = group.length ? group : field.parent();

        field.addClass('is-invalid');
        if (group.length) {
          group.addClass('error');
        }

        $('<div class="error-block"></div>').text(message).appendTo(target);
      }

      // Focus first invalid field so the user immediately sees what to fix.
      const firstInvalid = form.find('.is-invalid').first();
      if (firstInvalid.length) {
        firstInvalid.trigger('focus');
      }
    },

    /**
     * Hides the modal.
     */
    hide: function () {
      if (self.modal) {
        self.modal.modal('hide');
      }
    },

    /**
     * Flash the given message as an error.
     *
     * @param message the message
     */
    flash_error: function (message) {
      self.flash(message, 'alert-error');
    },

    /**
     * Flash the given message as a success.
     *
     * @param message the message
     */
    flash_success: function (message) {
      self.flash(message, 'alert-success');
    },

    /**
     * Create a flash and display it.
     *
     * @param message the flash message
     * @param category the type of flash to show, this is used as the css class
     */
    flash: function (message, category) {
      const flash = $('<div class="alert alert-dismissible fade show" role="alert"></div>');
      flash.addClass(category);
      flash.html(message);
      flash.append(
        $('<button type="button" class="btn-close close" data-bs-dismiss="alert"></button>')
          .attr('aria-label', self.messages.closeLabel),
      );
      $('.flash-messages').append(flash);
      if (self.options.scrollOnFlash) {
        self.scrollToFlashMessages();
      }
    },

    scrollToFlashMessages: function () {
      const container = $('.flash-messages');
      if (!container.length) {
        return;
      }
      const targetTop = Math.max(container.first().offset().top - 16, 0);
      const prefersReducedMotion =
        window.matchMedia &&
        window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      if (prefersReducedMotion) {
        window.scrollTo(0, targetTop);
        return;
      }
      $('html, body').stop(true).animate({ scrollTop: targetTop }, 250);
    },

    /**
     * Event handler for clicking on the element.
     *
     * @private
     */
    _onClick: function (event) {
      event.preventDefault();
      self.show();
    },
  };
});

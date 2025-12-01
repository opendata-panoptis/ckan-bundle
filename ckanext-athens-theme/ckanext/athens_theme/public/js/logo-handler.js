// Athens Theme Logo Handler
document.addEventListener('DOMContentLoaded', function() {
    const logo = document.querySelector('.site-logo');
    if (logo) {
        // Fallback σε περίπτωση που το λογότυπο δεν φορτώσει
        logo.onerror = function() {
            console.warn('Could not load Athens logo');
            this.src = '/base/images/placeholder-logo.png';
        };

        // Προσθήκη κλάσης για το animation εμφάνισης
        logo.onload = function() {
            this.classList.add('logo-loaded');
        };
    }
});


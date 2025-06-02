// Main JavaScript for the migration application

document.addEventListener('DOMContentLoaded', function() {
    // File input validation
    const fileInputs = document.querySelectorAll('input[type="file"]');
    
    fileInputs.forEach(input => {
        input.addEventListener('change', function() {
            const file = this.files[0];
            if (file) {
                const extension = file.name.split('.').pop().toLowerCase();
                if (!['xlsx', 'xls'].includes(extension)) {
                    alert('Please select a valid Excel file (.xlsx or .xls)');
                    this.value = '';
                }
            }
        });
    });
    
    // Form submission
    const form = document.querySelector('form');
    if (form) {
        form.addEventListener('submit', function() {
            // Show loading state
            const submitButton = this.querySelector('button[type="submit"]');
            if (submitButton) {
                submitButton.disabled = true;
                submitButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Processing...';
            }
        });
    }
});

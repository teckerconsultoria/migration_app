// JavaScript para Migration App

document.addEventListener('DOMContentLoaded', function() {
    // Manipula??o de ?rea de upload com drag and drop
    const uploadArea = document.querySelector('.upload-area');
    if (uploadArea) {
        const fileInput = document.querySelector('input[type="file"]');
        
        uploadArea.addEventListener('click', function() {
            fileInput.click();
        });
        
        fileInput.addEventListener('change', function() {
            if (this.files && this.files[0]) {
                handleFile(this.files[0]);
            }
        });
        
        uploadArea.addEventListener('dragover', function(e) {
            e.preventDefault();
            uploadArea.classList.add('dragover');
        });
        
        uploadArea.addEventListener('dragleave', function() {
            uploadArea.classList.remove('dragover');
        });
        
        uploadArea.addEventListener('drop', function(e) {
            e.preventDefault();
            uploadArea.classList.remove('dragover');
            
            if (e.dataTransfer.files && e.dataTransfer.files[0]) {
                fileInput.files = e.dataTransfer.files;
                handleFile(e.dataTransfer.files[0]);
            }
        });
        
        function handleFile(file) {
            const fileNameElement = document.getElementById('file-name');
            if (fileNameElement) {
                fileNameElement.textContent = file.name;
            }
            
            const fileTypeElement = document.getElementById('file-type');
            if (fileTypeElement) {
                fileTypeElement.textContent = file.type || 'Tipo desconhecido';
            }
            
            const fileSizeElement = document.getElementById('file-size');
            if (fileSizeElement) {
                fileSizeElement.textContent = formatFileSize(file.size);
            }
            
            // Mostrar pr?-visualiza??o se for uma imagem
            const previewElement = document.getElementById('file-preview');
            if (previewElement && file.type.match('image.*')) {
                const reader = new FileReader();
                reader.onload = function(e) {
                    previewElement.src = e.target.result;
                    previewElement.style.display = 'block';
                };
                reader.readAsDataURL(file);
            } else if (previewElement) {
                previewElement.style.display = 'none';
            }
        }
    }
    
    // Fun??o para formatar o tamanho do arquivo
    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
    
    // Inicializa??o de tooltips Bootstrap
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Auto-fechamento de alertas
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach(function(alert) {
        setTimeout(function() {
            const closeButton = alert.querySelector('.btn-close');
            if (closeButton) {
                closeButton.click();
            }
        }, 5000);
    });
});

// static/script.js
document.addEventListener('DOMContentLoaded', function() {
    const dropArea = document.getElementById('dropArea');
    const fileInput = document.getElementById('fileInput');
    const fileName = document.getElementById('fileName');
    const uploadButton = document.getElementById('uploadButton');
    const progressContainer = document.querySelector('.progress-container');
    const progressBar = document.getElementById('progressBar');
    const progressStatus = document.getElementById('progressStatus');
    const results = document.getElementById('results');
    const resultContent = document.getElementById('resultContent');
    
    // Prevent default drag behaviors
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, preventDefaults, false);
    });
    
    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }
    
    // Highlight drop area when dragging over it
    ['dragenter', 'dragover'].forEach(eventName => {
        dropArea.addEventListener(eventName, highlight, false);
    });
    
    ['dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, unhighlight, false);
    });
    
    function highlight() {
        dropArea.classList.add('highlight');
    }
    
    function unhighlight() {
        dropArea.classList.remove('highlight');
    }
    
    // Handle dropped files
    dropArea.addEventListener('drop', handleDrop, false);
    
    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        handleFiles(files);
    }
    
    fileInput.addEventListener('change', function() {
        handleFiles(this.files);
    });
    
    function handleFiles(files) {
        if (files.length > 0) {
            const file = files[0];
            if (file.name.toLowerCase().endsWith('.csv')) {
                fileName.textContent = file.name;
                uploadButton.disabled = false;
            } else {
                fileName.textContent = 'Please select a CSV file';
                uploadButton.disabled = true;
            }
        }
    }
    
    uploadButton.addEventListener('click', function() {
        if (fileInput.files.length === 0) return;
        
        const file = fileInput.files[0];
        const compression = document.getElementById('compression').value;
        
        // Show progress
        progressContainer.style.display = 'block';
        results.style.display = 'none';
        uploadButton.disabled = true;
        
        // Create FormData
        const formData = new FormData();
        formData.append('file', file);
        formData.append('compression', compression);
        
        // Upload file
        const xhr = new XMLHttpRequest();
        xhr.open('POST', '/compress-and-forward/', true);
        
        xhr.upload.onprogress = function(e) {
            if (e.lengthComputable) {
                const percentComplete = (e.loaded / e.total) * 100;
                progressBar.style.width = percentComplete + '%';
                progressStatus.textContent = `Uploading: ${Math.round(percentComplete)}%`;
            }
        };
        
        xhr.onload = function() {
            if (xhr.status === 200) {
                progressStatus.textContent = 'Processing CSV data...';
                progressBar.style.width = '100%';
                
                // Handle successful response
                const response = JSON.parse(xhr.responseText);
                results.style.display = 'block';
                resultContent.textContent = response.go_backend_response;
                
                // Reset button
                uploadButton.disabled = false;
            } else {
                progressStatus.textContent = 'Error: ' + xhr.statusText;
                uploadButton.disabled = false;
            }
        };
        
        xhr.onerror = function() {
            progressStatus.textContent = 'Upload failed. Please try again.';
            uploadButton.disabled = false;
        };
        
        xhr.send(formData);
    });
});
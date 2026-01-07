CREATE TABLE IF NOT EXISTS bronze.processed_files (
    file_name VARCHAR(255) PRIMARY KEY,        
    table_name VARCHAR(100) NOT NULL,          
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    record_count INTEGER                       
);
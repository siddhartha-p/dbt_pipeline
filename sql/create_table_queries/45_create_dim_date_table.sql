CREATE TABLE if not exists gold.dim_date (
    date_key INTEGER PRIMARY KEY,  
    date_actual DATE NOT NULL,        
    day_name varchar(20) NOT NULL,        
    day_of_week smallint NOT NULL,     
    day_of_month smallint NOT NULL,     
    month_actual smallint NOT NULL,     
    month_name varchar(20) NOT NULL,       
    quarter smallint NOT NULL,    
    year smallint NOT NULL,   
    is_weekend BOOLEAN NOT NULL,     
    is_holiday BOOLEAN DEFAULT FALSE,
    is_last_day_of_month BOOLEAN NOT NULL      
);
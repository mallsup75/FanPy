-- Source System Table
CREATE TABLE Source_System (
    system_id INT PRIMARY KEY AUTO_INCREMENT,
    system_name VARCHAR(50) NOT NULL,
    description TEXT
);

-- Source Customer Record Table
CREATE TABLE Source_Customer_Record (
    record_id INT PRIMARY KEY AUTO_INCREMENT,
    system_id INT NOT NULL,
    source_customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    address VARCHAR(255),
    phone_number VARCHAR(20),
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (system_id) REFERENCES Source_System(system_id),
    UNIQUE (system_id, source_customer_id)
);

-- Golden Record Table
CREATE TABLE Golden_Record (
    golden_record_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    address VARCHAR(255),
    phone_number VARCHAR(20),
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    status ENUM('Active', 'Inactive', 'Pending') DEFAULT 'Active'
);

-- Record Mapping Table
CREATE TABLE Record_Mapping (
    mapping_id INT PRIMARY KEY AUTO_INCREMENT,
    golden_record_id INT NOT NULL,
    record_id INT NOT NULL,
    confidence_score DECIMAL(5,2) DEFAULT 1.00,
    mapping_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (golden_record_id) REFERENCES Golden_Record(golden_record_id),
    FOREIGN KEY (record_id) REFERENCES Source_Customer_Record(record_id),
    UNIQUE (golden_record_id, record_id)
);

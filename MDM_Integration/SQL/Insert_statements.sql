-- Insert Source Systems
INSERT INTO Source_System (system_name, description) VALUES
('CRM', 'Customer Relationship Management'),
('eCommerce', 'Online Store Platform'),
('ERP', 'Enterprise Resource Planning');

-- Insert Source Customer Records (sample subset for brevity)
INSERT INTO Source_Customer_Record (system_id, source_customer_id, first_name, last_name, email, address, phone_number) VALUES
(1, '101', 'John', 'Doe', 'john.doe@example.com', '123 Main St, NY', '555-1234'),
(1, '102', 'Jane', 'Smith', 'jane.smith@example.com', '456 Oak St, CA', '555-5678'),
(1, '103', 'Alice', 'Johnson', 'alice.johnson@example.com', '789 Pine St, TX', '555-9101'),
(1, '104', 'Bob', 'Brown', 'bob.brown@example.com', '101 Maple St, FL', '555-1122'),
(2, '101', 'John', 'Doe', 'johnny.doe@ecommerce.com', '123 Main St, NY', '555-1234'),
(2, '102', 'Jane', 'Smith', 'jane_smith@store.com', '456 Oak St, CA', '555-5678'),
(2, '301', 'Anna', 'Williams', 'anna.williams@ecommerce.com', '12 Sunset Blvd, CA', '555-3010'),
(3, '101', 'John', 'Doe', 'john.doe@company.com', '123 Main St, NY', '555-1234'),
(3, '104', 'Bob', 'Brown', 'bob.brown@example.com', '101 Maple St, FL', '555-1122'),
(3, '201', 'Michael', 'Brown', 'michael.brown@erp.com', '1020 Maple Ave, CA', '555-2010');

-- Insert Golden Records (after deduplication)
INSERT INTO Golden_Record (first_name, last_name, email, address, phone_number) VALUES
('John', 'Doe', 'john.doe@example.com', '123 Main St, NY', '555-1234'), -- GR1: Merged John Doe
('Jane', 'Smith', 'jane.smith@example.com', '456 Oak St, CA', '555-5678'), -- GR2: Jane Smith
('Alice', 'Johnson', 'alice.johnson@example.com', '789 Pine St, TX', '555-9101'), -- GR3: Alice Johnson
('Bob', 'Brown', 'bob.brown@example.com', '101 Maple St, FL', '555-1122'), -- GR4: Bob Brown
('Anna', 'Williams', 'anna.williams@ecommerce.com', '12 Sunset Blvd, CA', '555-3010'), -- GR5: Anna Williams
('Michael', 'Brown', 'michael.brown@erp.com', '1020 Maple Ave, CA', '555-2010'); -- GR6: Michael Brown

-- Insert Record Mappings (linking source records to Golden Records)
INSERT INTO Record_Mapping (golden_record_id, record_id, confidence_score) VALUES
(1, 1, 0.99), -- John Doe (CRM) -> GR1
(1, 5, 0.95), -- John Doe (eCommerce) -> GR1
(1, 8, 0.97), -- John Doe (ERP) -> GR1
(2, 2, 0.99), -- Jane Smith (CRM) -> GR2
(2, 6, 0.96), -- Jane Smith (eCommerce) -> GR2
(3, 3, 1.00), -- Alice Johnson (CRM) -> GR3
(4, 4, 0.99), -- Bob Brown (CRM) -> GR4
(4, 9, 0.99), -- Bob Brown (ERP) -> GR4
(5, 7, 1.00), -- Anna Williams (eCommerce) -> GR5
(6, 10, 1.00); -- Michael Brown (ERP) -> GR6

CREATE TABLE bank_customers (
	customer_id int PRIMARY KEY,
	name varchar(255),
	gender varchar(50),
	age int,
	region varchar(50),
	job_classification varchar(255),
	balance float
);


-- Insert data to table bank-customers
INSERT INTO bank_customers (customer_id, name, gender, age, region)
VALUES (1, 'Simon', 'Male', 21, 'England'),
	(2, 'Jasmine', 'Female', 34, 'Northern Ireland'),
	(3, 'Liam', 'Male', 46, 'England'),
	(4, 'Trevor', 'Male', 32, 'Wales'),
	(5, 'Ava', 'Female', 30, 'Scotland');
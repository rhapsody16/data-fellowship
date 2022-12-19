CREATE SOURCE CONNECTOR `jdbc-connector` WITH(
  "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
  "connection.url"='jdbc:postgresql://localhost:5432/YOUR_USERNAME',
  "mode"='bulk',
  "topic.prefix"='jdbc-',
  "key"='username');

 -- Create Streams
CREATE STREAM stream_table(
    customer_id INT,
    name VARCHAR,
    gender VARCHAR,
    age INT,
    region VARCHAR,
) WITH (kafka_topics='jdbc_bank_customers', value_format='json', partitions=1);

-- Create materialized table
CREATE TABLE final_table as
    SELECT st.name, count(*) as transaction_times, sum(balance) as total_balance 
    FROM stream_table st
    GROUP BY st.name
    emit changes;

-- insert data to stream table
INSERT INTO stream_table (customer_id, name, gender, age, region) VALUES (1, 'Simon', 'Male', 21, 'England');
INSERT INTO stream_table (customer_id, name, gender, age, region) VALUES (2, 'Jasmine', 'Female', 34, 'Northern Ireland');
INSERT INTO stream_table (customer_id, name, gender, age, region) VALUES (3, 'Liam', 'Male', 46, 'England');
INSERT INTO stream_table (customer_id, name, gender, age, region) VALUES (4, 'Trevor', 'Male', 32, 'Wales');
INSERT INTO stream_table (customer_id, name, gender, age, region) VALUES (5, 'Ava', 'Female', 30, 'Scotland');
INSERT INTO stream_table (customer_id, name, gender, age, region) VALUES (1, 'Simon', 'Male', 21, 'England');

-- get final_table data 
select * from final_table WHERE region == 'England';
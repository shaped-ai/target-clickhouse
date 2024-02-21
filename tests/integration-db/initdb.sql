CREATE TABLE simple_table (
    id INTEGER,
    updated_at DATE,
    name VARCHAR
)
ENGINE = MergeTree()
PRIMARY KEY id;

INSERT INTO simple_table VALUES (1, '2023-10-22 10:00:00', 'test1');
INSERT INTO simple_table VALUES (2, '2023-10-22 11:00:00', 'test3');

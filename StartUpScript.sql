INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:/start_db' AS my_data (DATA_PATH '/startup/data');
CREATE TABLE my_data.main.demo (key STRING,value STRING,partition INT);
ALTER TABLE my_data.main.demo SET PARTITIONED BY (partition);
INSERT INTO my_data.main.demo VALUES('k00', 'v00', 0),('k01', 'v01', 0),('k51', 'v51', 1),('k61', 'v61', 1);
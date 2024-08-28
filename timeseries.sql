USE timeseries;
-- DESCRIBE aapl_timeseries;

-- Changing column names ---------------------------

-- ALTER TABLE aapl_timeseries
-- CHANGE COLUMN `Unnamed: 0` RecordDate DATE;

-- ALTER TABLE aapl_timeseries
-- CHANGE COLUMN `1. open` Open FLOAT;

-- ALTER TABLE aapl_timeseries
-- CHANGE COLUMN `2. high` High FLOAT;

-- ALTER TABLE aapl_timeseries
-- CHANGE COLUMN `3. low` Low FLOAT;

-- ALTER TABLE aapl_timeseries
-- CHANGE COLUMN `4. close` Close FLOAT;

-- ALTER TABLE aapl_timeseries
-- CHANGE COLUMN `5. volume` Volume INT;

-- Changing column data type ---------------------------

-- ALTER TABLE aapl_timeseries
-- MODIFY COLUMN Ticker VARCHAR(5);

-- ALTER TABLE aapl_timeseries
-- MODIFY COLUMN RecordDate DATE;

-- Adding ID column ---------------------------

-- ALTER TABLE aapl_timeseries
-- ADD COLUMN ID INT;

-- SET @row_number = 0;
-- UPDATE aapl_timeseries
-- SET ID = (@row_number := @row_number + 1)

-- ALTER TABLE aapl_timeseries
-- ADD PRIMARY KEY (ID);

-- ALTER TABLE aapl_timeseries
-- MODIFY COLUMN ID INT AUTO_INCREMENT;

-- Adding InsertedAt column ---------------------------

-- ALTER TABLE aapl_timeseries
-- ADD COLUMN InsertedAt DATETIME DEFAULT CURRENT_TIMESTAMP;

-- ALTER TABLE aapl_timeseries
-- MODIFY COLUMN InsertedAt DATETIME DEFAULT CURRENT_TIMESTAMP;

-- Setting columns to not take null values ---------------------------

-- ALTER TABLE aapl_timeseries
-- MODIFY `Open` FLOAT NOT NULL;

-- ALTER TABLE aapl_timeseries
-- MODIFY `High` FLOAT NOT NULL;

-- ALTER TABLE aapl_timeseries
-- MODIFY `Low` FLOAT NOT NULL;

-- ALTER TABLE aapl_timeseries
-- MODIFY `Close` FLOAT NOT NULL;

-- ALTER TABLE aapl_timeseries
-- MODIFY `Volume` INT NOT NULL;

-- ALTER TABLE aapl_timeseries
-- MODIFY `Ticker` VARCHAR(5) NOT NULL;

-- ALTER TABLE aapl_timeseries
-- MODIFY `InsertedAt` DATETIME NOT NULL;

-- ALTER TABLE aapl_timeseries
-- MODIFY `RecordDate` DATE NOT NULL;

-- DESCRIBE aapl_timeseries;

-- SELECT * FROM aapl_timeseries LIMIT 5;

-- SELECT COUNT(*) FROM aapl_timeseries;


 
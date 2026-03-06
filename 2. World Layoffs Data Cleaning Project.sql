-- Data Cleaning

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any Columns ** not from raw dataset**


SELECT * 
FROM layoffs;

-- Remove Duplicates
CREATE TABLE layoffs_staging				--- create stage table to make changes to data
LIKE layoffs;

INSERT layoffs_staging						--- copy data from orginal table (layoffs) to stage table (layoffs_staging)
SELECT *
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(							--- use 'ROW_NUMBER()' (WINDOW FUNCTION) to identify duplicate records
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS						--- used CTE to create a temporary table to find any duplicate rows
(SELECT *,									
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, 
funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;						---any row with row_num >1 is identified as duplicate

SELECT *								---example used to find duplicate
FROM layoffs_staging
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging2` (				---create a new table and add one column 'row_num' to filter duplicates
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT										---added 'row_num' to new table 'layoffs_staging2'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;							---new table shows an additional column 'row_num' with empty records

INSERT INTO layoffs_staging2					---copy all columns from orginal table into new duplicate table 'layoffs_staging2'
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, 
funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *								---- row_num shows duplicate rows
FROM layoffs_staging2
WHERE row_num > 1;

DELETE								--- deletes duplicate rows
FROM layoffs_staging2
WHERE row_num > 1;


-- Standardizing Data (finding issues in data)

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1; 

-- make changes to crypto, cyrpto currency, and cryptocurrencty
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)  -- REMOVES '.' AFTER 'UNITED STATES'
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- NULL VALUES

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''; 


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num; -- deletes column 'row_num'

SELECT *
FROM layoffs_staging2;



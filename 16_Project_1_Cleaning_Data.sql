-- Data Cleaning
USE world_layoffs;
Select * 
FROM layoffs;

-- 1 Remove Duplicates
-- 2 Standardize the data
-- 3 Null values or blank values
-- 4 Remove any columns
 
 
 -- Create a copy of the raw data on which all the work will be done
 
 Create TABLE layoffs_staging # Creates a table with the same columns
 LIKE layoffs; 
 
Select * 
FROM layoffs_staging;

INSERT layoffs_staging # Inserts all the data into the second column
SELECT *
FROM layoffs;

-- Remove duplicates

WITH duplicate_cte AS
(
Select *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry, total_laid_off, percentage_laid_off, 'date',stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1
; 

SELECT *
FROM layoffs_staging
WHERE company = 'Oyster'
;

INSERT INTO layoffs_staging2
Select *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry, total_laid_off, percentage_laid_off, 'date',stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num>1
;
DELETE
FROM layoffs_staging2
WHERE row_num>1
;
SELECT *
FROM layoffs_staging2
WHERE row_num>1
;
-- Standardizing data
 -- 1. Removing extra space
SELECT company, (trim(company))
FROM layoffs_staging2;
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1 
;

-- 2. Renaming same industries with the same name
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 3. Finding irregularities in columns


SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1 
;
SELECT DISTINCT(country)
FROM layoffs_staging2
WHERE country like '%.'
ORDER BY 1 
;
SELECT DISTINCT country , TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1 
;
UPDATE layoffs_staging2
SET country =  TRIM(TRAILING '.' FROM country)
WHERE country LIKE '%.'
;

SELECT *
FROM layoffs_staging2

;
-- 4. Format date column

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET  `date` =  STR_TO_DATE(`date`, '%m/%d/%Y')

;
SELECT `date`
FROM layoffs_staging2
; 

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 5. Dealing with null values

SELECT *
FROM layoffs_staging2

;
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry =''
;

SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry is null
;

SELECT DISTINCT *
FROM layoffs_staging2
WHERE industry is null
OR industry = ''
;

SELECT  t1.company, t1.industry, t2.company, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL
;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL
;

-- 6. Removing columns and rows 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;
-- If these columns are null, the data can't be used, so maybe it needs to be deleted
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2
;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
;

-- 
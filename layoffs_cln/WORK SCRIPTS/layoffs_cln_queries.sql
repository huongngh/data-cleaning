-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary



with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry
			,total_laid_off 
			,percentage_laid_off
			,date 
			,stage, country
			,funds_raised_millions) as row_num
from layoffs_staging
)

select * 
from duplicate_cte
where row_num > 1


insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry
			,total_laid_off 
			,percentage_laid_off
			,date 
			,stage, country
			,funds_raised_millions) as row_num
from layoffs_staging


select * from layoffs_staging2
where row_num>1

delete
from layoffs_staging2
where row_num>1

-- 2. standardize data and fix errors
select * from layoffs_staging2

select company, trim(company)
from layoffs_staging2

update layoffs_staging2
set company = trim(company)

select distinct industry
from layoffs_staging2
order by 1

select * from layoffs_staging2
where industry like 'Crypto%'

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'

select distinct country
from layoffs_staging2
order by 1

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%'


-- 3. Look at null values and see what 

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null

select * from layoffs_staging2
where industry is null 

select * from layoffs_staging2
where company like 'Bally%'

--prepare to backfill missing values

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company = t2.company
where (t1.industry is null)
	and t2.industry is not null
	
update layoffs_staging2 
set industry = NULL
where industry = ''

--backfill missing values

update layoffs_staging2 as t1
set industry = t2.industry 
from layoffs_staging2 as t2 
where t1.company = t2.company
	and t1.industry is null 
	and t2.industry is not null
		
delete
from layoffs_staging2
where total_laid_off is null
	and percentage_laid_off is null


-- 4. remove any columns and rows that are not necessary

alter table layoffs_staging2
drop column row_num
use layoff;
SET SQL_SAFE_UPDATES = 0;
SET GLOBAL local_infile = 1;
SHOW GLOBAL VARIABLES LIKE 'local_infile';
###################### MEMASUKAN DATA DARI CSV KE SQL ##########################################
CREATE TABLE layoffS (
  company VARCHAR(100),
  location VARCHAR(100),
  industry VARCHAR(100),
  total_laid_off INT,
  percentage_laid_off DECIMAL(5,2),
  date VARCHAR(100),
  stage VARCHAR(100),
  country VARCHAR(100),
  funds_raised_millions DECIMAL(10,2)
);
Select * from layoffs;
LOAD DATA LOCAL INFILE 'C:/Data housing/layoffs.csv'
INTO TABLE layoffs
FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- step 1. membuat table baru (agar data aslinya utuh)
create table layoffs_staging
like layoffs;
insert into layoffs_staging
select
	*
from
	layoffs;
######################################### 2. MEMBUANG ROWS YANG DUPLIKAT
select
	*,
	row_number() over
    (partition by company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    funds_raised_millions) as row_num
from
	layoffs_staging;
-- 3. check rows yang lebih dari 1 menggunakan CTE
with duplicates as
(
select
	*,
	row_number() over
    (partition by company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    funds_raised_millions) as row_num
from
	layoffs_staging
)
select
	*
from
	duplicates
where
	row_num > 1;
-- delete row yang lebih dari 1 harus membuat table baru
create table layoffs_staging2 (
company VARCHAR(100),
  location VARCHAR(100),
  industry VARCHAR(100),
  total_laid_off INT,
  percentage_laid_off DECIMAL(5,2),
  date VARCHAR(100),
  stage VARCHAR(100),
  country VARCHAR(100),
  funds_raised_millions DECIMAL(10,2),
  row_num int);
  
select * from layoffs_staging2;
insert into layoffs_staging2
select
	*,
	row_number() over
    (partition by company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    funds_raised_millions) as row_num
from
	layoffs_staging;
    
select
	*
from
	layoffs_staging2
where
	row_num > 1;
-- mendelete row_num > 1;
delete
from
	layoffs_staging2
where
	row_num > 1;

######################################2 standardize data
-- 1. check tiap kolom
select
	distinct(industry)
from
	layoffs_staging2
where
	industry is not null and industry != ''
order by
	1;
-- 2.membuang spasi di depan (company)
select
	distinct(company),
    trim(company)
from
	layoffs_staging2
order by
	1;

update layoffs_staging2
set company = trim(company);


-- menyatukan nama yang mirip (industry = crypto)
select * from layoffs_staging2;
select
	distinct(industry)
from
	layoffs_staging2
where
	industry is not null and industry != ''
order by
	1;

select
	distinct(industry)
from
	layoffs_staging2
where
	industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry LIKE 'Crypto%';

-- check kolom stage
select
	distinct(stage)
from
	layoffs_staging2
where
	stage is not null
order by
	1;

-- check kolom country
select
	distinct(country)
from
	layoffs_staging2
#where
	#stage is not null
order by
	1;

select
	distinct(country)
from
	layoffs_staging2
#where
	#country like 'United States%'
order by
	1;

update layoffs_staging2
set country = 'United States'
where country LIKE 'United_states%';

-- mengganti format date
select
	date,
    str_to_date(date, '%m/%d/%Y')
from
	layoffs_staging2;

update layoffs_staging2
set date = str_to_date(date, '%m/%d/%Y');

############ remove null or blank values (including populate)
-- check tiap kolom
select
	distinct(industry)
from
	layoffs_staging2
where
	industry is null or industry = '';
    
select
	*
from
	layoffs_staging2 t1
join
	layoffs_staging2 t2 on t1.company = t2.company
    and t1.location = t2.location
where
	(t1.industry is null or t1.industry = '') or
    t2.industry is not null or t2.industry != '';

-- buat blank values menjadi null values dulu
select
	industry
from
	layoffs_staging2
where
	industry is null;

update layoffs_staging2
set industry = null
where industry = '';

select
	*
from
	layoffs_staging2 t1
join
	layoffs_staging2 t2 on t1.company = t2.company
    and t1.location = t2.location
where
	t1.industry is null and
    t2.industry is not null;
    
update layoffs_staging2 t1
join layoffs_staging2 t2 on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- MENGHAPUS ROWS YANG TOTAL LAID OFF DAN PERCENTAGE LAID OFF NYA NULL
select
	*
from
	layoffs_staging2
where
	total_laid_off is null and percentage_laid_off is null;
    
delete
from
	layoffs_staging2
where
	total_laid_off is null and percentage_laid_off is null;
    
select * from layoffs_staging2;

################# drop the unnecessary column
alter table layoffs_staging2
drop column row_num;
    

    

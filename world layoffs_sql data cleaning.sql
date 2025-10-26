/*in this project, we are analyzing world layoffs that occurred between 2020 and 2023.
 Our goal is to understand the trends in these layoffs across companies,their locations, different industries and countries.
 We aim to first clean the data, then perform exploratory data analysis (EDA) to generate insights. */

#CLEANING THE DATA

SELECT * FROM world_layoffs.layoffs;

create table layoffs2
like layoffs
;

insert layoffs2
select *
from layoffs
;

select *
from layoffs2
;

#we created a second table because it is not advisable to use the raw data since we will make changes in the database

#step we will take
-- 1)removing duplicates
-- 2)standartize the data 
-- 3)checking null values or blanks values (populating if we can)
-- 4)removing any columns if necessary

  
-- 1)removing duplicates

select *
from layoffs2;

select * ,
row_number () over(partition by company, location, industry, total_laid_off, 
					percentage_laid_off , `date`, stage, country, funds_raised_millions) as row_num
from layoffs2
;                    

with duplicate_cte as
(select * ,
row_number () over(partition by company, location, industry, total_laid_off, 
					percentage_laid_off , `date`, stage, country, funds_raised_millions) as row_num
from layoffs2
)
select *
from duplicate_cte
where row_num >1
;

#we are checking every row from the output just to confirm

select *
from layoffs2
where company = 'yahoo'
;

#we are creating a third table where row_number exists as a real column to delete the duplicates

CREATE TABLE `layoffs3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs3
select * ,
row_number () over(partition by company, location, industry, total_laid_off, 
					percentage_laid_off , `date`, stage, country, funds_raised_millions) as row_num
from layoffs2
;

#now we can delete the duplicate rows

select *
from layoffs3
where row_num >1
;

delete 
from layoffs3
where row_num >1
;

/*if the sql gives "error code 1175:you are using safe update mode", use the code down below 
or go to edit -> preferences -> SQL ediyor -> uncheck the box and start the mySQL again*/

set sql_safe_updates = 0;


-- 2)standartize the data (finding issues and fixing them)

select*
from layoffs3;

#we are applying trim function to text columns to remove any leading or trailing spaces

select trim(company), trim(location), trim(industry), trim(stage), trim(country)
from layoffs3
where company != ' '
and location != ' '
and industry != ' '
and stage != ' '
and country != ' '
;

update layoffs3 
set company = trim(company),
	location = trim(location),
    industry = trim(industry),
    stage = trim(stage),
    country = trim(country);
    
# now we are going to check most of the columns to see if there are any mistakes

-- company 

select distinct company 
from layoffs3
order by country;

select company
from layoffs3
where company like 'Impossible Foods%'
;

update layoffs3
set company = 'impossible foods'
where company like 'impossible foods%'
;

select company
from layoffs3
where company like 'Digital Currency Gruop'
;

update layoffs3
set company = 'Digital currency group'
where company like 'Digital currency gruop'
;



-- starting with the first column (company), after noticing that there are no null or blank spaces, we began by correcting some spelling errors

-- location (I individually checked and updated each record but they could also be updated under a single query using case statements, as both methods would work

select distinct location
from layoffs3
;

select location
from layoffs3
where location is null
or location = ' '
;

select location
from layoffs3
where location = 'DÃ¼sseldorf'
or location = 'dusseldorf'
;

update layoffs3
set location = 'Düsseldorf'
where location = 'DÃ¼sseldorf'
or location = 'dusseldorf'
;

select location
from layoffs3
where location = 'FlorianÃ³polis'
;

update layoffs3
set location = 'Florianópolis'
where location = 'FlorianÃ³polis'
;

select *
from layoffs3
where location = 'MalmÃ¶'
or location = 'Malmo'
;

update layoffs3 
set location = 'Malmö'
where location = 'MalmÃ¶'
or location = 'Malmo'
;

select location , country
from layoffs3
where location = 'Shenzen'
;

update layoffs3
set location = 'Shenzhen'
where location = 'Shenzen'
;

-- industry 

select distinct industry
from layoffs3
order by industry;

# we will take care of the null and blank spaces in the next section

select industry
from layoffs3
where industry like 'crypto%'
;

update layoffs3
set industry = 'Crypto'
where industry in ('CryptoCurrency' , 'crypto currency')
;

-- country

select distinct country
from layoffs3
order by country
;

select distinct country
from layoffs3
where country like 'united states%'
;

update layoffs3
set country = trim(trailing '.' from country)
;

/* First, we check the data types of the variables to see if there are any incorrect classifications.
Then we convert the wrongly classified columns into their correct types  */

show columns from layoffs3 ; 

-- we need to fix date to date and percentage_laid_off to float 

select `date`,
str_to_date(`date` , '%m/%d/%Y') 
from layoffs3
;

update layoffs3
set `date` = str_to_date(`date` , '%m/%d/%Y') ;

alter table layoffs3
modify column `date` date ;

select * 
from layoffs3;

alter table layoffs3
modify column percentage_laid_off float;

-- 3) null values or blank values

select industry
from layoffs 
order by industry
;

update layoffs3
set industry = null
where char_length(industry) = '0'
;

select *
from world_layoffs.layoffs3
where industry IS null
order by industry;

select *
from layoffs3
where company in ('juul' , 'carvana' , 'airbnb')
or company like 'bally%'
;

-- we can populate values where company = Airbnb, carvana and juul  

select i1.company , i1.industry , i2.industry
from layoffs3 as i1
join layoffs3 as i2
	on i1.company = i2.company
    and i1.location = i2.location
    and i1.country = i2.country
where i1.industry is not null 
and i2.industry is null
;

update layoffs3 as i1 
join layoffs3 as i2
	on i1.company = i2.company
    and i1.location = i2.location
    and i1.country = i2.country
set i1.industry = i2.industry
where i1.industry is not null
and i2.industry is null
;

-- 4)removing any columns if necessary

-- since we are looking the data for world layoffs, rows where both total_laid_offs and percentage_laid_off have null values are not valuable to us

select *
from layoffs3
where total_laid_off is null
and percentage_laid_off is null
;

delete
from layoffs3
where total_laid_off is null
and percentage_laid_off is null
;

-- nowe we can also delete row_num column

alter table layoffs3
drop column row_num
;

select *
from layoffs3;




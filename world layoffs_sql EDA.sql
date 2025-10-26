#EDA 

-- In this section we are just going to explore the data to try and understand patterns or find trends

select  max(total_laid_off), min(total_laid_off)
from layoffs3;

#or 

select *, 
	(select  max(total_laid_off)
    from layoffs3),
    (select min(total_laid_off)
    from layoffs3)
from layoffs3
;

select max(percentage_laid_off), 
min(percentage_laid_off)
from layoffs3
;

-- companies where the whole of the recorded industry was laid off :

select * 
from layoffs3
where percentage_laid_off = 1
order by company asc
;

select company, industry, country, funds_raised_millions
from layoffs3
where percentage_laid_off = 1
order by funds_raised_millions desc
;

select company, sum(total_laid_off) as sum_total
from layoffs3
group by company
order by sum_total desc
;

#in total vs in one day

select company,
max(total_laid_off) as max_total
from layoffs3
group by company
order by max_total desc
;

select min(`date`) , max(`date`)
from layoffs3
;

select industry, sum(total_laid_off) as sum_total
from layoffs3
group by industry
order by sum_total desc
;

select country, sum(total_laid_off) as sum_total2
from layoffs3
group by country
order by sum_total2 desc
;

delimiter $$
create procedure `stage_and_date`()
BEGIN
	select year(`date`), sum(total_laid_off) as sum_total3
	from layoffs3
	group by year(`date`)
	order by year(`date`) desc;
    select stage, sum(total_laid_off) as sum_total4
	from layoffs3 
	group by stage
    order by sum_total4 desc ;
end $$
delimiter ;

call stage_and_date();

-- we are now preparing for rolling total

select substring( `date`,1,7) as dates, sum(total_laid_off) 
from layoffs3 
where substring( `date`,1,7) is not null
group by dates
order by dates asc
;

with cte_rolling_t as (
	select substring( `date`,1,7) as dates, sum(total_laid_off) as total_offs
	from layoffs3 
	where substring( `date`,1,7) is not null
	group by dates
	order by dates asc
) 
select dates, total_offs,
sum(total_offs) over(order by dates asc) as dates_rolling_t
from cte_rolling_t 
group by dates
order by dates 
;

select company , year(`date`), sum(total_laid_off)
from layoffs3
group by company, year(`date`)
order by year(`date`)
;

-- we are using a CTE to determine which companies had the highest total laif off numbers in different years. 
-- the goal is to retrieve the top 5 companies for each year

with company_year (company, years, sum_total) as (
select company , year(`date`), sum(total_laid_off) 
from layoffs3
group by company, year(`date`)
), c_y_rank as (
select * ,
dense_rank() over(partition by years order by sum_total desc) as rank_1
from company_year
where years is not null
and sum_total is not null
) 
select *
from c_y_rank
where rank_1 <= 5
;

select * 
from layoffs3;


-- biggest layoffs across different years based on country and industry

-- we are writing our queries before butting them in a CTE. After that, we will be putting them into a stored procedure for two different output

select country, 
year(`date`),
sum(total_laid_off)
from layoffs3
group by country , year(`date`)
order by year(`date`)
;


with cte2 (country, years, sum_total) as (
select country,
year(`date`),
sum(total_laid_off)
from layoffs3
group by country , year(`date`) 
order by year(`date`)
) , country_layoffs as (
select *,
dense_rank() over(partition by years order by sum_total desc) as ranking
from cte2
where years is not null
and sum_total is not null
) 
select * 
from country_layoffs
where ranking = 1
;

delimiter $$
drop procedure if exists `country_industry_layoffs`;
create procedure `country_industry_layoffs`()
BEGIN
	with cte2 (country, years, sum_total) as (
	select country,
	year(`date`),
	sum(total_laid_off)
	from layoffs3
	group by country , year(`date`) 
	order by year(`date`)
	) , country_layoffs as (
	select *,
	dense_rank() over(partition by years order by sum_total desc) as ranking
	from cte2
	where years is not null
	and sum_total is not null
	) 
	select * 
	from country_layoffs
	where ranking = 1 
    ;
	with cte3 (industry, years, sum_total)as (
	select industry , 
	year(`date`) , 
	sum(total_laid_off)
	from layoffs3
	group by industry , year(`date`)
	order by year(`date`) 
	) , industry_layoffs as (
	select *,
	dense_rank() over(partition by years order by sum_total desc) ranking
	from cte3
	where years is not null
	and sum_total is not null
	)
	select *
	from industry_layoffs
	where ranking = 1
	;
END $$
delimiter ;

call country_industry_layoffs();

/* percentage of layoffs by the industry
which sectors lost the highest percentage of employees in each country */

with country_industry_p_l_o as (
select country, industry, percentage_laid_off,
dense_rank() over(partition by country order by percentage_laid_off desc) as ranking
from layoffs3
group by country, industry, percentage_laid_off
)
select* 
from country_industry_p_l_o
where ranking = 1
and percentage_laid_off is not null
order by country
;

-- layoff comparision : How much did the number of layoffs increase annualy?

select year(`date`) as years,
sum(total_laid_off)
from layoffs3
where year(`date`) is not null
group by years
order by years
;

with layoff_comparision as (
select year(`date`) as years,
sum(total_laid_off) as sum_total,
lag(sum(total_laid_off)) over(order by year(`date`)) as previous
from layoffs3
where year(`date`) is not null
group by years
order by years
) 
select *, round(((sum_total-previous)/previous) * 100,2) as growth_rate
from layoff_comparision
;

-- which month had the peak of layoffs

select substring(`date`, 1,7) as months,
sum(total_laid_off) as sum_total
from layoffs3
group by months
order by sum_total desc
limit 1
;

-- the top companies that raised the most funds and their corresponding layoffs

with company_funds as (
select company, funds_raised_millions,
sum(total_laid_off) as sum_total
from layoffs3
group by company, funds_raised_millions
)
select *
from company_funds
where sum_total is not null
order by funds_raised_millions desc
;


CREATE TABLE coviddeaths(
	iso_code varchar, continent	varchar, location varchar, date	date, population bigint, total_cases int, 
	new_cases int, new_cases_smoothed decimal, total_deaths int, new_deaths int, new_deaths_smoothed decimal,
	total_cases_per_million decimal, new_cases_per_million decimal, new_cases_smoothed_per_million decimal,	total_deaths_per_million decimal,
	new_deaths_per_million decimal,	new_deaths_smoothed_per_million	decimal, reproduction_rate decimal, icu_patients decimal, icu_patients_per_million decimal,
	hosp_patients decimal, hosp_patients_per_million decimal, weekly_icu_admissions decimal, weekly_icu_admissions_per_million decimal,weekly_hosp_admissions decimal,
	weekly_hosp_admissions_per_million decimal);
	
CREATE TABLE covidvaccination (
	iso_code varchar, continent	varchar, location varchar, date date, new_tests int, total_tests int,
	total_tests_per_thousand decimal, new_tests_per_thousand decimal, new_tests_smoothed int, new_tests_smoothed_per_thousand decimal,
	positive_rate decimal, tests_per_case decimal, tests_units varchar,	total_vaccinations int,	people_vaccinated int,	people_fully_vaccinated	int,
	new_vaccinations int, new_vaccinations_smoothed int, total_vaccinations_per_hundred decimal, people_vaccinated_per_hundred decimal,
	people_fully_vaccinated_per_hundred decimal, new_vaccinations_smoothed_per_million int,	stringency_index decimal, population_density decimal,
	median_age decimal,	aged_65_older decimal,	aged_70_older decimal,	gdp_per_capita decimal,	extreme_poverty decimal, cardiovasc_death_rate decimal,
	diabetes_prevalence	decimal, female_smokers decimal, male_smokers decimal,	handwashing_facilities decimal, hospital_beds_per_thousand decimal, 
	life_expectancy	decimal, human_development_index decimal);

COPY coviddeaths
FROM 'C:\Program Files\PostgreSQL\15\data\CovidDeaths.csv' DELIMITER ',' CSV HEADER;

COPY covidvaccination
FROM 'C:\Program Files\PostgreSQL\15\data\CovidVaccinations.csv' DELIMITER ',' CSV HEADER;

SELECT *
FROM coviddeaths
where continent is not null

SELECT *
FROM covidvaccination


-- Choosing the columns i will be working with
SELECT location, date,  population, total_deaths, new_cases, total_cases
FROM coviddeaths
ORDER BY 2,3

--Checking the percentage of deaths for each country
SELECT location, date, total_cases, total_deaths, cast (total_deaths as float) / total_cases * 100 AS "death_percentage"
FROM coviddeaths
WHERE continent is not NULL
ORDER BY 2,3

-- Looking at the likelihood of covid deaths in Nigeria
SELECT location, date, total_cases, total_deaths, cast (total_deaths as decimal) / total_cases AS "death_percentage"
FROM coviddeaths
WHERE location like '%Nigeria%'
AND continent is not null
ORDER BY 2,3

--Checking the total % of the population that contracted COVID in Nigeria
SELECT location, date, population, total_cases, cast (total_cases as decimal) / population * 100 AS "%population infected"
FROM coviddeaths
WHERE location like '%Nigeria%'
ORDER BY 2,3

-- Checking the counntries with the highest infection rate
SELECT location, population, MAX (total_cases) AS "HighestInfectionCount", MAX (cast(total_cases as decimal) / population) * 100
AS "perpopulation_infected"
FROM coviddeaths
WHERE continent is not NULL
GROUP BY location, population
ORDER BY "perpopulation_infected" DESC;

-- Check for the highest death count
SELECT location, MAX (total_deaths) AS "deathcount"
FROM coviddeaths
WHERE continent is NULL
GROUP BY location
ORDER BY "deathcount" DESC;

SELECT location, MAX (total_deaths) AS "deathcount"
FROM coviddeaths
WHERE continent is not NULL
GROUP BY location
ORDER BY "deathcount" DESC;

-- Global values around the world
SELECT date, SUM(new_cases) AS "Total cases", SUM (new_deaths) AS "Total deaths", SUM(new_deaths) / SUM (new_cases)*100
AS "Death percentage"
FROM coviddeaths
WHERE continent is not NULL
GROUP BY date
ORDER BY 1,2

-- Total new cases, death and percentage from the whole world.
SELECT SUM(new_cases) AS "Total cases", SUM (new_deaths) AS "Total deaths", SUM(new_deaths)/SUM (new_cases)*100
AS "Death percentage"
FROM coviddeaths
WHERE continent is not NULL
ORDER BY 1,2

-- WORKING WITH THE VACCINATION TABLE NOW
SELECT *
FROM covidvaccination

-- Join both tables together
SELECT *
FROM coviddeaths AS dea
JOIN covidvaccination AS vac 
	ON dea.date = vac.date
	AND dea.location = vac.location;

-- Total population VS total vaccinations around the world
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM (new_vaccinations) 
OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rollingvaccination
FROM coviddeaths AS dea
JOIN covidvaccination AS vac 
	ON dea.date = vac.date
	AND dea.location = vac.location
WHERE dea.continent is not NULL
--AND vac.new_vaccinations is not NULL
ORDER BY 2,3;

-- Using CTE
With popvsvac (continent, location, date, population, new_vaccinations, rollingvaccination)
AS
(
	SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM (new_vaccinations) 
OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rollingvaccination
FROM coviddeaths AS dea
JOIN covidvaccination AS vac 
	ON dea.date = vac.date
	AND dea.location = vac.location
WHERE dea.continent is not NULL
--AND vac.new_vaccinations is not NULL
ORDER BY 2,3
)

SELECT *, (rollingvaccination/population)*100 AS "percentage vaccinated"
FROM popvsvac;

-- Using a Temporary table

DROP TABLE if exists percentvaccinated;
CREATE TABLE percentvaccinated (
	continent varchar, location varchar, date date, population int, new_vaccinations int,
	rollingvaccination decimal);
	
INSERT INTO percentvaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM (new_vaccinations) 
OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rollingvaccination
FROM coviddeaths AS dea
JOIN covidvaccination AS vac 
	ON dea.date = vac.date
	AND dea.location = vac.location
WHERE dea.continent is not NULL
--AND vac.new_vaccinations is not NULL
ORDER BY 2,3

SELECT *, (rollingvaccination/population)*100 AS "percentage vaccinated"
FROM percentvaccinated

-- Number 


-- Creating VIEW to store data for later visualization
Create view percentvaccinated as 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM (new_vaccinations) 
OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rollingvaccination
FROM coviddeaths AS dea
JOIN covidvaccination AS vac 
	ON dea.date = vac.date
	AND dea.location = vac.location
WHERE dea.continent is not NULL
--AND vac.new_vaccinations is not NULL
ORDER BY 2,3


-- Data exported for visualization
-- Total new cases, death and percentage from the whole world.
SELECT SUM(new_cases) AS "Total cases", SUM (new_deaths) AS "Total deaths", SUM(new_deaths)/SUM (new_cases)*100
AS "Death percentage"
FROM coviddeaths
WHERE continent is not NULL
ORDER BY 1,2

SELECT location, SUM (new_deaths) AS "Totaldeaths"
FROM coviddeaths
WHERE continent is NULL
and location not in ('World','European Union', 'International')
Group by location
ORDER BY "Totaldeaths" DESC

-- Checking the counntries with the highest infection rate
SELECT location, population, MAX (total_cases) AS "HighestInfectionCount", MAX (cast(total_cases as decimal) / population) * 100
AS "perpopulation_infected"
FROM coviddeaths
GROUP BY location, population
ORDER BY "perpopulation_infected" DESC;

-- Checking the counntries with the highest infection rate (with date included)
SELECT location, population, date, MAX (total_cases) AS "HighestInfectionCount", MAX (cast(total_cases as decimal) / population) * 100
AS "perpopulation_infected"
FROM coviddeaths
GROUP BY location, population, date
ORDER BY "perpopulation_infected" DESC;
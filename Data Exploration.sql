-- SELECT * 
-- FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
-- ORDER BY 3,4
-- LIMIT 10;

-- SELECT *
-- FROM groovy-legacy-397104.Data_Portfolio_01.CovidVaccinations_modified
-- ORDER BY 3,4
-- LIMIT 10;

--------------------------------------------------------------
--------------------------------------------------------------

-- Investigate Total Cases vs Total Deaths for Each Location

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
ORDER BY 1,2;

-- Investigate Total Cases vs Total Deaths for Australia
-- DeathPercentage represents the probability of dying from Covid in Australia during that period
SELECT location, date, total_cases, new_cases, total_deaths, (total_deaths / total_cases) *100 AS DeathPercentage
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
WHERE location LIKE 'Australia'
ORDER BY 1,2;

-- Investigate Total Cases vs Total Population for Australia
SELECT location, date, total_cases, new_cases, total_deaths, (total_cases / population) *100 AS CovidPercentage
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
WHERE location LIKE 'Australia'
ORDER BY 1,2;

--------------------------------------------------------------
--------------------------------------------------------------

-- Investigate Locations with Highest Infection Rate Compared to Population
SELECT location, date, total_cases, new_cases, total_deaths, (total_cases / population) *100 AS CovidPercentage
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
ORDER BY 6 DESC
LIMIT 100;

-- Investigate Locations with Highest Infection Rate Compared to Population (version 1)
SELECT location, population, MAX(total_cases) AS HighestInfectionCount, MAX(total_cases / population) *100 AS CovidPercentage
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
GROUP by location, population
ORDER BY CovidPercentage DESC
LIMIT 100;

-- Investigate Locations with Highest Infection Rate Compared to Population (version 2, including the date)

SELECT cd.location, 
       cd.date, 
       cd.population,
       cd.total_cases AS highest_infection_count, 
       (cd.total_cases / cd.population) *100 AS covid_percentage
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified cd
JOIN (
    -- Retrive the highest infection count per location
    SELECT location, MAX(total_cases) AS highest_infection_count
    FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
    GROUP BY location
) sub
ON cd.location = sub.location AND cd.total_cases = sub.highest_infection_count
ORDER BY covid_percentage DESC
LIMIT 100;

--------------------------------------------------------------
--------------------------------------------------------------
--Investigate Locations with Highest Death Count per Population
SELECT location, MAX(total_deaths) AS highest_death_count
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY highest_death_count DESC
LIMIT 50

--Group by continent (v1.0)
-- SELECT continent, MAX(total_deaths) AS highest_death_count
-- FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
-- WHERE continent IS NOT NULL
-- GROUP BY continent
-- ORDER BY highest_death_count DESC
-- LIMIT 50

--Group by contineny (version 2.0)
SELECT location, MAX(total_deaths) AS highest_death_count
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
WHERE continent IS NULL
GROUP BY location
ORDER BY highest_death_count DESC
LIMIT 50

--------------------------------------------------------------
--------------------------------------------------------------

--Investigate Locations with Highest Death Rate with Infection

--------------------------------------------------------------
      --     In order to retrieve distinct locations            
      --          ROW_NUMBER() is applied                       
      --  The function is suggested by ChatGPT                 
--------------------------------------------------------------

WITH RankedDeaths AS (
    SELECT cd.location, 
           cd.date, 
           cd.total_cases, 
           cd.total_deaths AS highest_death_count, 
           (cd.total_deaths / NULLIF(cd.total_cases, 0)) * 100 AS death_percentage,
           ROW_NUMBER() OVER (PARTITION BY cd.location ORDER BY (cd.total_deaths / NULLIF(cd.total_cases, 0)) * 100 DESC) AS rank
    FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified cd
    JOIN (
        -- Retrieve the highest death count per location
        SELECT location, MAX(total_deaths) AS highest_death_count
        FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
        GROUP BY location
    ) sub
    ON cd.location = sub.location AND cd.total_deaths = sub.highest_death_count
)
SELECT location, date, total_cases, highest_death_count, death_percentage
FROM RankedDeaths
WHERE rank = 1
ORDER BY death_percentage DESC
LIMIT 100;

--------------------------------------------------------------
--------------------------------------------------------------

--Global Numbers
--Daily New Cases
SELECT date, SUM(new_cases) AS global_new_cases
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY global_new_cases DESC
LIMIT 50

SELECT date, 
        SUM(new_cases) AS global_new_cases, 
        SUM(new_deaths) AS global_new_deaths, 
        (SUM(new_deaths) / SUM(new_cases))*100 AS global_deaths_percentage
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
WHERE continent IS NOT NULL
GROUP BY date
--HAVING SUM(new_deaths) IS NOT NULL
ORDER BY date
LIMIT 50

SELECT  SUM(new_cases) AS global_new_cases, 
        SUM(new_deaths) AS global_new_deaths, 
        (SUM(new_deaths) / SUM(new_cases))*100 AS global_deaths_percentage
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified
WHERE continent IS NOT NULL
--HAVING SUM(new_deaths) IS NOT NULL
LIMIT 50      

--------------------------------------------------------------
--------------------------------------------------------------
----------Investigate Total Population vs Vaccination Data---
--------------------------------------------------------------
--------------------------------------------------------------

SELECT *
FROM groovy-legacy-397104.Data_Portfolio_01.CovidVaccinations_modified
ORDER BY 4
LIMIT 50

--------------------------------------------------------------
--------------------------------------------------------------

SELECT dea.continent, 
        dea.location, 
        dea.date, 
        dea.population, 
        vac.new_vaccinations,
        SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_vaccinated
FROM groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified dea
JOIN groovy-legacy-397104.Data_Portfolio_01.CovidVaccinations_modified vac
ON dea.location = vac.location
  AND dea.date = vac.date
WHERE vac.new_vaccinations IS NOT NULL
  AND vac.continent IS NOT NULL
-- WHERE vac.continent IS NOT NULL -- without considering vaccinations records are null
ORDER BY 2,3
LIMIT 200

--------------------------------------------------------------
-- Use CTE to Calculate the Rolling Percentage of People  
-- Who Have Been Vaccinated
--------------------------------------------------------------
WITH VacPercentage AS (
    SELECT dea.continent, 
           dea.location, 
           dea.date, 
           dea.population, 
           vac.new_vaccinations,
           SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.date) AS rolling_vaccinated
    FROM `groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified` dea
    JOIN `groovy-legacy-397104.Data_Portfolio_01.CovidVaccinations_modified` vac
    ON dea.location = vac.location
       AND dea.date = vac.date
    WHERE vac.new_vaccinations IS NOT NULL
      AND dea.continent IS NOT NULL
)

SELECT *, (rolling_vaccinated/population)*100
FROM VacPercentage
ORDER BY location, date
LIMIT 200;

--------------------------------------------------------------
-- Use Temp Table to Calculate the Rolling Percentage of People  
-- Who Have Been Vaccinated
--------------------------------------------------------------
DROP TABLE IF EXISTS groovy-legacy-397104.Data_Portfolio_01.VacPercentage;
CREATE TABLE groovy-legacy-397104.Data_Portfolio_01.VacPercentage(
continent string,
location string,
date datetime,
population numeric,
new_vaccinations numeric,
rolling_vaccinated numeric);

INSERT INTO groovy-legacy-397104.Data_Portfolio_01.VacPercentage
SELECT dea.continent, 
        dea.location, 
        dea.date, 
        dea.population, 
        vac.new_vaccinations,
        SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.date) AS rolling_vaccinated
FROM `groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified` dea
JOIN `groovy-legacy-397104.Data_Portfolio_01.CovidVaccinations_modified` vac
ON dea.location = vac.location
    AND dea.date = vac.date
WHERE vac.new_vaccinations IS NOT NULL
    AND dea.continent IS NOT NULL

SELECT *, (rolling_vaccinated/population)*100
FROM groovy-legacy-397104.Data_Portfolio_01.VacPercentage
ORDER BY location, date
LIMIT 200;

--------------------------------------------------------------
-- Creating View to Store Data for Later Visualisations  
--------------------------------------------------------------
DROP TABLE IF EXISTS groovy-legacy-397104.Data_Portfolio_01.VacPercentage;
CREATE VIEW groovy-legacy-397104.Data_Portfolio_01.VacPercentage AS
SELECT dea.continent, 
        dea.location, 
        dea.date, 
        dea.population, 
        vac.new_vaccinations,
        SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.date) AS rolling_vaccinated
FROM `groovy-legacy-397104.Data_Portfolio_01.CovidDeaths_modified` dea
JOIN `groovy-legacy-397104.Data_Portfolio_01.CovidVaccinations_modified` vac
ON dea.location = vac.location
    AND dea.date = vac.date
WHERE vac.new_vaccinations IS NOT NULL
    AND dea.continent IS NOT NULL

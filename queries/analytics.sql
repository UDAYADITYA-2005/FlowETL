-- analytics.sql
-- 10 analytical SQL queries on the loaded census table.
-- Run against: data/processed/india_census.db  |  Table: india_state_census

-- ── Q1: Top 10 states by development index ───────────────────────────────────
SELECT
    state,
    ROUND(dev_index, 4)              AS dev_index,
    ROUND(literacy_rate_overall, 2)  AS literacy_pct,
    ROUND(per_capita_income_inr)     AS income_inr,
    ROUND(unemployment_rate_pct, 2)  AS unemployment_pct
FROM india_state_census
ORDER BY dev_index DESC
LIMIT 10;

-- ── Q2: Rank states by literacy within urban population quartile (WINDOW FN) ─
SELECT
    state,
    ROUND(literacy_rate_overall, 2)  AS literacy_pct,
    ROUND(urban_population_pct, 2)   AS urban_pct,
    NTILE(4) OVER (ORDER BY urban_population_pct) AS urban_quartile,
    RANK()   OVER (
        PARTITION BY NTILE(4) OVER (ORDER BY urban_population_pct)
        ORDER BY literacy_rate_overall DESC
    ) AS literacy_rank_in_quartile
FROM india_state_census;

-- ── Q3: Gender literacy gap — top 10 worst states ────────────────────────────
SELECT
    state,
    ROUND(literacy_rate_male, 2)    AS male_lit_pct,
    ROUND(literacy_rate_female, 2)  AS female_lit_pct,
    ROUND(gender_literacy_gap, 2)   AS gap
FROM india_state_census
ORDER BY gender_literacy_gap DESC
LIMIT 10;

-- ── Q4: States above national average in both literacy AND income (SUBQUERY) ─
SELECT
    state,
    ROUND(literacy_rate_overall, 2) AS literacy_pct,
    ROUND(per_capita_income_inr)    AS income_inr
FROM india_state_census
WHERE literacy_rate_overall > (SELECT AVG(literacy_rate_overall) FROM india_state_census)
  AND per_capita_income_inr  > (SELECT AVG(per_capita_income_inr)  FROM india_state_census)
ORDER BY dev_index DESC;

-- ── Q5: Agriculture vs services vs manufacturing — sector breakdown ───────────
SELECT
    state,
    ROUND(agriculture_employment_pct, 1)    AS agriculture_pct,
    ROUND(services_employment_pct, 1)       AS services_pct,
    ROUND(manufacturing_employment_pct, 1)  AS manufacturing_pct,
    CASE
        WHEN agriculture_employment_pct > 50 THEN 'Agriculture-dominant'
        WHEN services_employment_pct    > 40 THEN 'Services-dominant'
        ELSE 'Mixed economy'
    END AS economy_type
FROM india_state_census
ORDER BY agriculture_employment_pct DESC;

-- ── Q6: Running total of population (cumulative, ordered by population) ───────
SELECT
    state,
    total_population,
    SUM(total_population) OVER (ORDER BY total_population DESC) AS cumulative_population,
    ROUND(
        100.0 * SUM(total_population) OVER (ORDER BY total_population DESC)
        / SUM(total_population) OVER (), 2
    ) AS cumulative_pct
FROM india_state_census
ORDER BY total_population DESC;

-- ── Q7: Population density segmentation using CASE WHEN ──────────────────────
SELECT
    state,
    ROUND(population_density, 1) AS density,
    CASE
        WHEN population_density >= 1000 THEN 'Very High (>=1000)'
        WHEN population_density >=  500 THEN 'High (500-999)'
        WHEN population_density >=  200 THEN 'Medium (200-499)'
        ELSE 'Low (<200)'
    END AS density_band,
    COUNT(*) OVER (
        PARTITION BY CASE
            WHEN population_density >= 1000 THEN 'Very High'
            WHEN population_density >=  500 THEN 'High'
            WHEN population_density >=  200 THEN 'Medium'
            ELSE 'Low'
        END
    ) AS states_in_band
FROM india_state_census
ORDER BY population_density DESC;

-- ── Q8: COALESCE demo — safe labour force metric ──────────────────────────────
SELECT
    state,
    COALESCE(labour_force_participation_pct, 0.0)  AS safe_labour_force_pct,
    COALESCE(unemployment_rate_pct, 0.0)           AS safe_unemployment_pct,
    ROUND(
        COALESCE(labour_force_participation_pct, 0)
        - COALESCE(unemployment_rate_pct, 0), 2
    ) AS effective_employment_pct
FROM india_state_census
ORDER BY effective_employment_pct DESC;

-- ── Q9: CTE — two-step: compute averages, then compare each state ─────────────
WITH national_avg AS (
    SELECT
        AVG(literacy_rate_overall)   AS avg_literacy,
        AVG(per_capita_income_inr)   AS avg_income,
        AVG(unemployment_rate_pct)   AS avg_unemployment
    FROM india_state_census
)
SELECT
    s.state,
    ROUND(s.literacy_rate_overall - n.avg_literacy, 2)    AS literacy_vs_avg,
    ROUND(s.per_capita_income_inr - n.avg_income)          AS income_vs_avg,
    ROUND(s.unemployment_rate_pct - n.avg_unemployment, 2) AS unemployment_vs_avg
FROM india_state_census s
CROSS JOIN national_avg n
ORDER BY literacy_vs_avg DESC;

-- ── Q10: ROW_NUMBER to get top 3 states per economy type ─────────────────────
WITH classified AS (
    SELECT
        state,
        dev_index,
        CASE
            WHEN agriculture_employment_pct > 50 THEN 'Agriculture-dominant'
            WHEN services_employment_pct    > 40 THEN 'Services-dominant'
            ELSE 'Mixed economy'
        END AS economy_type
    FROM india_state_census
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY economy_type ORDER BY dev_index DESC) AS rn
    FROM classified
)
SELECT economy_type, state, ROUND(dev_index, 4) AS dev_index
FROM ranked
WHERE rn <= 3
ORDER BY economy_type, rn;

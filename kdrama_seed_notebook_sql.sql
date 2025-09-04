-- Databricks notebook source
-- COMMAND ----------
-- K-Drama Dataset: SQL Seed Notebook
-- This SQL notebook is designed to run on a Serverless SQL warehouse.
-- It loads the CSVs using READ_FILES, creates (or replaces) tables,
-- then demonstrates joins and basic analytics.

-- HOW TO USE
-- 1) Upload the CSVs to DBFS (e.g., /FileStore/kdramas/)
--    Files required:
--      - netflix_korean_tv_dramas_titles_enhanced.csv
--      - netflix_korean_tv_dramas_episodes.csv
--      - netflix_korean_tv_dramas_data_dictionary.csv
-- 2) Set base_path below to the directory where you uploaded the files.
-- 3) Run cells top-to-bottom.
-- COMMAND ----------
-- =============================================
-- Configuration
-- =============================================
-- Update this path to where your files live in DBFS.
-- Example: 'dbfs:/FileStore/kdramas'
SET base_path = 'dbfs:/FileStore/kdramas';-- COMMAND ----------
-- =============================================
-- Load CSVs into TEMP VIEWS using READ_FILES
-- =============================================
CREATE OR REPLACE TEMP VIEW titles_v_raw AS
SELECT *
FROM READ_FILES(
  CONCAT('${base_path}', '/netflix_korean_tv_dramas_titles_enhanced.csv'),
  FORMAT => 'CSV',
  HEADER => true,
  INFER_SCHEMA => true
);

CREATE OR REPLACE TEMP VIEW episodes_v_raw AS
SELECT *
FROM READ_FILES(
  CONCAT('${base_path}', '/netflix_korean_tv_dramas_episodes.csv'),
  FORMAT => 'CSV',
  HEADER => true,
  INFER_SCHEMA => true
);

CREATE OR REPLACE TEMP VIEW dict_v_raw AS
SELECT *
FROM READ_FILES(
  CONCAT('${base_path}', '/netflix_korean_tv_dramas_data_dictionary.csv'),
  FORMAT => 'CSV',
  HEADER => true,
  INFER_SCHEMA => true
);

-- Quick peek
SELECT * FROM titles_v_raw LIMIT 20;
-- COMMAND ----------
-- =============================================
-- Create (or replace) managed tables
-- (Optional: switch catalog/schema first, e.g., USE CATALOG main; USE SCHEMA default;)
-- =============================================
CREATE OR REPLACE TABLE kdrama_titles AS
SELECT * FROM titles_v_raw;

CREATE OR REPLACE TABLE kdrama_episodes AS
SELECT * FROM episodes_v_raw;

CREATE OR REPLACE TABLE kdrama_data_dictionary AS
SELECT * FROM dict_v_raw;-- COMMAND ----------
-- Inspect schemas
DESCRIBE TABLE kdrama_titles;
DESCRIBE TABLE kdrama_episodes;
DESCRIBE TABLE kdrama_data_dictionary;-- COMMAND ----------
-- =============================================
-- Join examples
-- =============================================
-- 1) Episodes joined with titles including maturity subratings
SELECT
  e.show_id,
  t.title,
  e.season_number,
  e.episode_number,
  e.episode_id,
  t.maturity_rating,
  t.subrating_violence,
  t.subrating_language,
  t.subrating_sex
FROM kdrama_episodes e
JOIN kdrama_titles t
  ON e.show_id = t.show_id
ORDER BY t.title, e.season_number, e.episode_number
LIMIT 200;-- COMMAND ----------
-- =============================================
-- Simple analytics
-- =============================================

-- Average runtime by maturity rating
SELECT
  maturity_rating,
  ROUND(AVG(per_episode_runtime_min), 1) AS avg_runtime_min
FROM kdrama_titles
GROUP BY maturity_rating
ORDER BY avg_runtime_min DESC;

-- Episodes per show
SELECT
  show_id,
  COUNT(*) AS ep_count
FROM kdrama_episodes
GROUP BY show_id
ORDER BY ep_count DESC;

-- Availability counts (training flags)
SELECT
  SUM(CASE WHEN available_us THEN 1 ELSE 0 END) AS titles_available_us,
  SUM(CASE WHEN available_ca THEN 1 ELSE 0 END) AS titles_available_ca,
  SUM(CASE WHEN available_uk THEN 1 ELSE 0 END) AS titles_available_uk,
  SUM(CASE WHEN available_kr THEN 1 ELSE 0 END) AS titles_available_kr,
  COUNT(*) AS total_titles
FROM kdrama_titles;-- COMMAND ----------
-- =============================================
-- Window functions & filtering
-- =============================================

-- Rank titles by episodes within each maturity rating
WITH t AS (
  SELECT title, maturity_rating, episodes,
         DENSE_RANK() OVER (PARTITION BY maturity_rating ORDER BY episodes DESC) AS rnk
  FROM kdrama_titles
)
SELECT * FROM t
WHERE rnk <= 5
ORDER BY maturity_rating, rnk, episodes DESC;

-- Titles with Strong violence subrating
SELECT title, genres, maturity_rating, subrating_violence
FROM kdrama_titles
WHERE subrating_violence = 'Strong'
ORDER BY title;-- COMMAND ----------
-- =============================================
-- Temp views for ad hoc exploration
-- =============================================
CREATE OR REPLACE TEMP VIEW titles_tv AS SELECT * FROM kdrama_titles;
CREATE OR REPLACE TEMP VIEW episodes_tv AS SELECT * FROM kdrama_episodes;

-- Example: Romance shows available in US
SELECT title, maturity_rating, available_us
FROM titles_tv
WHERE available_us = TRUE AND LOWER(genres) LIKE '%romance%'
ORDER BY title;

-- Example: Total minutes if you binge a show
SELECT
  t.title,
  t.per_episode_runtime_min * COUNT(e.episode_id) AS binge_minutes
FROM episodes_tv e
JOIN titles_tv t ON e.show_id = t.show_id
GROUP BY t.title, t.per_episode_runtime_min
ORDER BY binge_minutes DESC
LIMIT 25;
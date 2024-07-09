-- Databricks notebook source
CREATE OR REPLACE TABLE gold.ans.num_operadoras
USING DELTA AS (
  SELECT * FROM silver.ans.operadoras
  WHERE LOWER(MODALIDADE) NOT LIKE '%odonto%'
)

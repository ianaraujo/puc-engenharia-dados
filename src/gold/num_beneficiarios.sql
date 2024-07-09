-- Databricks notebook source
CREATE OR REPLACE TABLE gold.ans.num_beneficiarios
USING DELTA AS (
    SELECT CD_OPERADORA, NM_RAZAO_SOCIAL,
    CASE 
        WHEN DE_CONTRATACAO_PLANO LIKE 'Coletivo Empresarial%' THEN 'Coletivo Empresarial'
        WHEN DE_CONTRATACAO_PLANO LIKE 'Coletivo por Adesão%' THEN 'Coletivo por Adesão'
        WHEN DE_CONTRATACAO_PLANO LIKE 'Individual%' THEN 'Individual ou Familiar'
        ELSE 'Não identificado'
    END AS TIPO_CONTRATACAO_PLANO,
    SUM(QT_BENEFICIARIO_ATIVO) AS TOTAL_BENEFICIARIOS
FROM 
    silver.ans.beneficiarios_2024_05
GROUP BY 
    CD_OPERADORA, 
    NM_RAZAO_SOCIAL,
    TIPO_CONTRATACAO_PLANO
);

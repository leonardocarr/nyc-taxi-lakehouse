-- =============================================================================
-- Consultas de consumo sobre a camada gold.
--
-- O objetivo destas queries e provar que o star schema funciona: perguntas de
-- negocio reais respondidas com JOINs simples, sem CTE aninhada nem subquery
-- correlacionada. Se responder uma pergunta exige contorcionismo em SQL, o
-- modelo dimensional esta errado.
--
-- Ajuste o prefixo do catalogo conforme o seu ambiente (nyc_taxi. ou vazio).
-- =============================================================================

-- 1. Receita e volume por mes e forma de pagamento -----------------------------
SELECT
    d.year_month,
    p.payment_type_name,
    count(*)                              AS corridas,
    round(sum(f.total_amount), 2)         AS receita,
    round(avg(f.total_amount), 2)         AS ticket_medio,
    round(avg(f.tip_pct) * 100, 2)        AS gorjeta_media_pct
FROM gold.fct_trips f
JOIN gold.dim_date d         ON f.pickup_date_key  = d.date_key
JOIN gold.dim_payment_type p ON f.payment_type_key = p.payment_type_key
GROUP BY d.year_month, p.payment_type_name
ORDER BY d.year_month, receita DESC;


-- 2. Os 15 fluxos origem -> destino mais rentaveis ------------------------------
SELECT
    origem.borough  || ' / ' || origem.zone  AS embarque,
    destino.borough || ' / ' || destino.zone AS desembarque,
    count(*)                                 AS corridas,
    round(avg(f.trip_distance_km), 2)        AS km_medio,
    round(avg(f.trip_duration_min), 1)       AS minutos_medios,
    round(sum(f.total_amount), 2)            AS receita
FROM gold.fct_trips f
JOIN gold.dim_location origem  ON f.pickup_location_key  = origem.location_key
JOIN gold.dim_location destino ON f.dropoff_location_key = destino.location_key
GROUP BY 1, 2
HAVING count(*) > 100
ORDER BY receita DESC
LIMIT 15;


-- 3. Velocidade media por hora do dia: onde esta o congestionamento -------------
SELECT
    t.hour,
    t.day_period,
    d.day_type,
    count(*)                          AS corridas,
    round(avg(f.avg_speed_kmh), 2)    AS velocidade_media_kmh,
    round(avg(f.trip_duration_min), 1) AS duracao_media_min
FROM gold.fct_trips f
JOIN gold.dim_time t ON f.pickup_time_key = t.time_key
JOIN gold.dim_date d ON f.pickup_date_key = d.date_key
WHERE f.avg_speed_kmh BETWEEN 1 AND 120
GROUP BY t.hour, t.day_period, d.day_type
ORDER BY t.hour;


-- 4. Corridas de aeroporto vs. corridas urbanas --------------------------------
SELECT
    CASE WHEN l.is_airport THEN 'Aeroporto' ELSE 'Urbana' END AS tipo_embarque,
    r.rate_code_name,
    count(*)                          AS corridas,
    round(avg(f.trip_distance_km), 2) AS km_medio,
    round(avg(f.total_amount), 2)     AS ticket_medio,
    round(avg(f.tip_pct) * 100, 2)    AS gorjeta_media_pct
FROM gold.fct_trips f
JOIN gold.dim_location l  ON f.pickup_location_key = l.location_key
JOIN gold.dim_rate_code r ON f.rate_code_key       = r.rate_code_key
GROUP BY 1, 2
ORDER BY corridas DESC;


-- 5. Sazonalidade semanal: dia da semana x periodo do dia ----------------------
SELECT
    d.day_name,
    t.day_period,
    count(*)                      AS corridas,
    round(sum(f.total_amount), 2) AS receita
FROM gold.fct_trips f
JOIN gold.dim_date d ON f.pickup_date_key = d.date_key
JOIN gold.dim_time t ON f.pickup_time_key = t.time_key
GROUP BY d.day_of_week, d.day_name, t.day_period
ORDER BY d.day_of_week, t.day_period;


-- 6. Observabilidade: taxa de quarentena por mes -------------------------------
SELECT
    load_month,
    (SELECT count(*) FROM silver.fct_source_trips s WHERE s.load_month = q.load_month) AS aceitas,
    count(*)                                                                            AS rejeitadas,
    round(
        100.0 * count(*) /
        nullif(count(*) + (SELECT count(*) FROM silver.fct_source_trips s WHERE s.load_month = q.load_month), 0),
        3
    ) AS pct_rejeicao
FROM silver.fct_source_trips_quarantine q
GROUP BY load_month
ORDER BY load_month;


-- 7. Observabilidade: historico das expectativas de qualidade ------------------
SELECT run_at, run_id, target, expectation, severity, observed, expected, passed
FROM gold.dq_results
ORDER BY run_at DESC
LIMIT 50;


-- 8. Time travel do Delta: o que mudou entre duas versoes da fato --------------
-- DESCRIBE HISTORY gold.fct_trips;
-- SELECT count(*) FROM gold.fct_trips VERSION AS OF 0;
-- SELECT count(*) FROM gold.fct_trips VERSION AS OF 1;

-- Replace workspace.renewables_demo with your catalog.schema if different.

-- 1. Site performance by day
SELECT site, reading_date, energy_mwh, capacity_factor, availability
FROM workspace.renewables_demo.gold_site_daily_performance
ORDER BY site, reading_date;

-- 2. Best and worst site overall
SELECT site,
       ROUND(SUM(energy_mwh), 1)      AS total_energy_mwh,
       ROUND(AVG(capacity_factor), 3) AS avg_capacity_factor,
       ROUND(AVG(availability), 3)    AS avg_availability
FROM workspace.renewables_demo.gold_site_daily_performance
GROUP BY site
ORDER BY avg_capacity_factor DESC;

-- 3. Turbines that underperformed in strong wind
SELECT site, turbine_id, reading_date, strong_wind_hours, underperforming_hours, underperformance_rate
FROM workspace.renewables_demo.gold_turbine_daily_performance
WHERE underperforming_hours > 0
ORDER BY underperformance_rate DESC;

-- 4. Maintenance and downtime
SELECT * FROM workspace.renewables_demo.gold_site_maintenance_summary ORDER BY downtime_hours DESC;

-- 5. Data quality history
SELECT source, bronze_rows, quarantined, duplicates_removed, silver_rows, reject_rate, run_at
FROM workspace.renewables_demo.gold_data_quality_summary
ORDER BY run_at DESC;

-- 6. What was rejected and why
SELECT _dq_reason AS reason, COUNT(*) AS rows_rejected
FROM workspace.renewables_demo.silver_turbine_readings_quarantine
GROUP BY _dq_reason
ORDER BY rows_rejected DESC;


-- 2h
CREATE MATERIALIZED VIEW departures AS (
WITH 
flt_processed_sel AS ( -- Allows for simple change of source table and or number of rows analyzed.
	SELECT *
	FROM flt_processed AS flt
	WHERE (flt.flown) AND (flt.landplane) AND (NOT flt.local) AND (NOT flt.civ_reg_gnr) AND (flt.dep_arr=0)
), 
dep_t0 AS (
	SELECT flt.flt_sep_id, tstzrange(flt.flt_sep_first, flt.flt_sep_last) AS flt_range, flt_max.trwy AS final_trwy, max(flt.asrt) AS t0_predict
	FROM 
		flt_processed_sel AS flt
		LEFT JOIN
		flt_processed_sel AS flt_max
	ON flt.flt_sep_last = flt_max.t AND flt.flt_sep_id = flt_max.flt_sep_id
	GROUP BY flt.flt_sep_id, flt_range, flt_max.trwy
),
dep_t0_ast AS (
	SELECT * 
	FROM dep_t0 AS dep
	LEFT JOIN
	astra_derived AS astra
	ON (Split_part(dep.flt_sep_id, '_', 2) = Split_part(astra.f_id_sep, '_', 1)) AND (TO_TIMESTAMP(Split_part(astra.f_id_sep, '_', 2)::bigint) <@ dep.flt_range)
	WHERE (astra.t_taxi IS NOT NULL) AND (dep.t0_predict IS NOT NULL) AND (astra.type='dep') AND (dep.final_trwy = ANY(STRING_TO_ARRAY(astra.rwy, '/'::text)))
),
dep_t0_ast_t AS (
	SELECT *, dep.t0_predict - make_interval(mins := h.horizon) AS t_predict 
	FROM dep_t0_ast AS dep
	CROSS JOIN (SELECT unnest('{0, 30, 120, 180}'::int[]) AS horizon) AS h 
), 
dep_t0_ast_t_flt AS (
	SELECT dep.*, flt.trwy, flt.actype, flt.obt_predict, flt.depgnr, flt.sid, flt.center_crossing
	FROM dep_t0_ast_t AS dep
	LEFT JOIN
	flt_processed_sel AS flt
	ON (dep.flt_sep_id = flt.flt_sep_id) AND (dep.t_predict <@ flt.valid_range)
	WHERE flt.obt_predict IS NOT NULL
), 
flt_event_table AS (
	SELECT valid_range,
		CASE 
			WHEN dep_arr = 0 THEN obt_predict
			WHEN dep_arr = 1 THEN at_predict
		END AS t_event,
		civ_reg_gnr AS civil,
		(NOT civ_reg_gnr) AND (dep_arr = 0) AS com_dep,
		(NOT civ_reg_gnr) AND (dep_arr = 1) AS com_arr
	FROM flt_processed
	WHERE NOT (obt_predict IS NULL AND at_predict IS NULL)
),
dep_c AS (
	SELECT dep.flt_sep_id, dep.t_predict, sum(civil::int) AS n_civil, sum(com_dep::int) AS n_dep, sum(com_arr::int) AS n_arr
	FROM
		dep_t0_ast_t_flt AS dep
		LEFT JOIN
		flt_event_table AS flt_event
		ON (dep.t_predict <@ flt_event.valid_range) AND ((dep.obt_predict - INTERVAL '10 minute')  <= flt_event.t_event) AND ((dep.obt_predict + INTERVAL '10 minute') >= flt_event.t_event)
	GROUP BY dep.flt_sep_id, dep.t_predict
),
dep_t0_ast_t_flt_c AS (
	SELECT dep.*, n_civil, n_dep, n_arr
	FROM 
		dep_t0_ast_t_flt AS dep
		LEFT JOIN
		dep_c AS dep_c
		ON dep.flt_sep_id = dep_c.flt_sep_id AND dep.t_predict = dep_c.t_predict
), 
dep_t0_ast_t_flt_c_wtc AS (
	SELECT dep.*, i.wtc
	FROM 
		dep_t0_ast_t_flt_c AS dep
		LEFT JOIN icao AS i 
		ON dep.actype = i.designator
),
dep_t0_ast_t_flt_c_wtc_wfs AS (
	SELECT DISTINCT ON (dep.flt_sep_id, dep.t_predict) dep.*, wfs.rvr5000_1000, wfs.rvr1500_300, wfs.rvr550_200, wfs.rvr350, wfs.rvrcat, wfs.wind_dir, wfs.wind_dir_std, wfs.wind_spd, wfs.wind_spd_std, wfs.wind_stoten, wfs.temp, wfs.dew, wfs.snow, wfs.snow_heavy, wfs.rain_cool, wfs.cb, wfs.lightning, wfs.rvr5000_2000
	--, concat(wfs.forecast_id, '_', wfs.t) AS wfs_row_id, wfs.f_type -- for debugging to check match 
	FROM
		dep_t0_ast_t_flt_c_wtc AS dep
		LEFT JOIN
		wfs AS wfs
		ON dep.t_predict >= wfs.t_publish AND dep.obt_predict <@ wfs.valid_range
	ORDER BY dep.flt_sep_id ASC, dep.t_predict ASC, wfs.forecast_id DESC
), 
dep_t0_ast_t_flt_c_wtc_wfs_rwy AS (
	SELECT DISTINCT ON (dep.flt_sep_id, dep.t_predict) dep.*, rwy.plr1, rwy.plr2, rwy.ptr1, rwy.ptr2, rwy.alr1, rwy.alr2, rwy.atr1, rwy.atr2
	--, concat(rwy.t_publish, '_', rwy.valid_range) AS rwy_row_id -- for debugging to check match 
	FROM 
		dep_t0_ast_t_flt_c_wtc_wfs AS dep
		LEFT JOIN
		rwy_config AS rwy
		ON dep.t_predict >= rwy.t_publish AND dep.obt_predict <@ rwy.valid_range
	ORDER BY dep.flt_sep_id ASC, dep.t_predict ASC, rwy.t_publish DESC, rwy.wfs_id DESC
)
SELECT *
	, EXTRACT(DOY FROM (t_predict at time zone 'Europe/Amsterdam')) AS local_doy
	, EXTRACT(DOW FROM (t_predict at time zone 'Europe/Amsterdam')) AS local_dow
	, EXTRACT(HOUR FROM (t_predict at time zone 'Europe/Amsterdam'))*60 +  EXTRACT(MINUTE FROM (t_predict at time zone 'Europe/Amsterdam')) AS local_mod
	, EXTRACT(WEEK FROM (t_predict at time zone 'Europe/Amsterdam')) AS local_week
FROM dep_t0_ast_t_flt_c_wtc_wfs_rwy
) WITH DATA;
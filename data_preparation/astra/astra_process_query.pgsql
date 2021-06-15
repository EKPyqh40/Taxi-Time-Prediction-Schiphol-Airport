-- ASTRA DERIVED 12 min
CREATE MATERIALIZED VIEW astra_derived AS (
WITH 
astra_sel AS (
	SELECT 
		t, 
		f_id_sep, 
		CASE -- Adds an xramp: on the ramp but not in a redzone
			WHEN 'ramp'=ANY(locs) AND 'red_zone'!=ANY(locs) 
			THEN array_append(locs, 'xramp') 
			ELSE locs 
		END AS locs, 
		queue, 
		s_missed,
		EXTRACT( epoch FROM (lead(t) OVER (PARTITION BY f_id_sep ORDER BY t) - t)) AS t_entry, -- time spent in this state (loc/queue)
		lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t) AS locs_prev,
		CASE 
			WHEN (	
				(lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t)::text SIMILAR TO '%rwy%') 
				AND (locs::text NOT SIMILAR TO '%rwy%')
				AND (locs::text NOT SIMILAR TO '%air%')
				)
			THEN t 
		END AS runway_out,
		CASE 
			WHEN (
				(lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t)::text NOT SIMILAR TO '%red_zone%') 
				AND (locs::text SIMILAR TO '%red_zone%')
				) 
			THEN t 
		END AS red_zone_in,
		CASE 
			WHEN ( 
				(
					(lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t)::text NOT SIMILAR TO '%rwy%')
					OR
					(
						lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t)::text SIMILAR TO '%rwy%'
						AND lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t)::text SIMILAR TO '%air%'
					)
				)
				AND (locs::text SIMILAR TO '%rwy%')
				AND (locs::text NOT SIMILAR TO '%air%')
				) 
			THEN t 
		END AS runway_in,
		CASE 
			WHEN ( 
				(lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t)::text SIMILAR TO '%red_zone%') 
				AND (locs::text NOT SIMILAR TO '%red_zone%')
				) 
			THEN t 
		END AS red_zone_out,
		CASE 
			WHEN ( 
				(lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t)::text NOT SIMILAR TO '%air%') 
				AND (locs::text SIMILAR TO '%air%')
				) 
			THEN t 
		END AS air_in
	FROM astra_processed 
), 
astra_taxi_event_dict AS ( -- I honestly forgot why I did it this way (using jsonb), might be simplifiable
	SELECT
		f_id_sep, 
		CASE 
			WHEN (max(red_zone_out) < max(runway_in)) AND (min(runway_out) < min(red_zone_in))
			THEN jsonb_object('{{type, "double"}}')
			WHEN (min(runway_out) < min(red_zone_in))
			THEN jsonb_object(CONCAT(
				'{{t_taxi,', EXTRACT(epoch FROM (min(red_zone_in)-min(runway_out))), 
				'},{t_rwy,', EXTRACT(epoch FROM min(runway_out)),
				'},{t_red_zone,', EXTRACT(epoch FROM min(red_zone_in)),
				'},{type,"arr"}}')::text[])
			WHEN (max(red_zone_out) < max(runway_in))
			THEN jsonb_object(CONCAT(
				'{{t_taxi,', EXTRACT(epoch FROM max(runway_in) - max(red_zone_out)),
				'},{t_rwy,', EXTRACT(epoch FROM max(runway_in)),
				'},{t_red_zone,', EXTRACT(epoch FROM max(red_zone_out)),
				'},{type,"dep"}}')::text[])
		END AS arr_dep
        FROM astra_sel
        GROUP BY f_id_sep
), 
astra_taxi AS (
    SELECT 
		event_dict.f_id_sep, 
		(event_dict.arr_dep ->> 't_taxi')::int AS t_taxi, 
		event_dict.arr_dep ->> 'type' AS type,
		CASE -- COULD PROBABLY CONDENSE THIS
			WHEN (event_dict.arr_dep ->> 'type') = 'arr'
			THEN to_timestamp((event_dict.arr_dep ->> 't_rwy')::int)
			WHEN (event_dict.arr_dep ->> 'type') = 'dep'
			THEN to_timestamp((event_dict.arr_dep ->> 't_red_zone')::int)
		END AS t_taxi_start,
		CASE
			WHEN (event_dict.arr_dep ->> 'type') = 'arr'
			THEN to_timestamp((event_dict.arr_dep ->> 't_red_zone')::int)
			WHEN (event_dict.arr_dep ->> 'type') = 'dep'
			THEN to_timestamp((event_dict.arr_dep ->> 't_rwy')::int)
		END AS t_taxi_end,
        CASE
            WHEN (event_dict.arr_dep ->> 'type') = 'arr'
            THEN substring(astra_rwy.locs_prev::text, '[{|,]rwy_(.*?)[}|,]')
            WHEN (event_dict.arr_dep ->> 'type') = 'dep' 
            THEN substring(astra_rwy.locs::text, '[{|,]rwy_(.*?)[}|,]')
        END AS rwy,
		CASE
			WHEN (event_dict.arr_dep ->> 'type') = 'arr'
			THEN substring(astra_red_zone.locs::text, '[{|,]red_zone_(.*?)[}|,]')
			WHEN (event_dict.arr_dep ->> 'type') = 'dep' 
			THEN substring(astra_red_zone.locs_prev::text, '[{|,]red_zone_(.*?)[}|,]')
		END AS red_zone,
		astra_rwy.s_missed AS s_missed_rwy,
		astra_red_zone.s_missed AS s_missed_red_zone
	FROM
		astra_taxi_event_dict AS event_dict
		LEFT JOIN
		astra_sel AS astra_rwy
		ON 
            ((event_dict.arr_dep ->> 't_rwy')::double precision = EXTRACT(epoch FROM astra_rwy.t)) 
            AND (event_dict.f_id_sep = astra_rwy.f_id_sep)
		LEFT JOIN 
		astra_sel AS astra_red_zone
		ON
			((event_dict.arr_dep ->> 't_red_zone')::double precision = EXTRACT(epoch FROM astra_red_zone.t))
			AND (event_dict.f_id_sep = astra_red_zone.f_id_sep)
), 
astra_t_spent AS (
    SELECT f_id_sep, json_object_agg(loc, t_spent) AS t_spent
    FROM (
		SELECT *
		FROM (
			SELECT f_id_sep, loc, sum(t_entry) AS t_spent -- in each locs
			FROM (
				SELECT 
					f_id_sep, 
					unnest(locs) AS loc,  
					t_entry
				FROM astra_sel
			) AS locs_unnest
			WHERE loc IS NOT NULL
			GROUP BY f_id_sep, loc
		) AS t_spent_locs
		WHERE t_spent_locs.t_spent IS NOT NULL
        UNION
		SELECT *
		FROM (
			SELECT f_id_sep, loc, sum(t_entry) AS t_spent -- in each queue
			FROM (
				SELECT 
					f_id_sep, 
					concat(unnest(queue), '_queue') AS loc,
					t_entry 
				FROM astra_sel
			) AS bar
			WHERE loc IS NOT NULL
			GROUP BY f_id_sep, loc
		) AS t_spent_queue
		WHERE t_spent_queue.t_spent IS NOT NULL
    ) AS t_spent
    GROUP BY f_id_sep
), 
astra_derived AS (
	SELECT astra_taxi.*, astra_t_spent.t_spent 
    FROM astra_taxi 
    LEFT JOIN astra_t_spent 
    ON astra_taxi.f_id_sep = astra_t_spent.f_id_sep
)
SELECT * FROM astra_derived ORDER BY f_id_sep
) WITH DATA;
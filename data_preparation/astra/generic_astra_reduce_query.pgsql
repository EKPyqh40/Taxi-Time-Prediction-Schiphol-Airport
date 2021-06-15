CREATE MATERIALIZED VIEW "{name}" AS (
WITH RECURSIVE 
astra_sel AS(
	SELECT a.astra_id, a.t, a.x, a.y, a.gs, a.f_id FROM astra AS a WHERE (t>= to_timestamp({start}-{start_buffer})) AND (t < to_timestamp({end}))
), 
astra_t_prior AS (
	SELECT 
		t, 
		f_id, 
		EXTRACT(epoch FROM (astra_sel.t - lag(astra_sel.t) OVER (PARTITION BY astra_sel.f_id ORDER BY astra_sel.t))) AS t_prior -- f_id, t
	FROM astra_sel
), 
astra_sep_table AS ( -- Window Functions are not allowed in WHERE
	SELECT 
		f_id, 
		t AS t_start, 
		lead(t) OVER (PARTITION BY f_id ORDER BY t) AS t_end, -- f_id, t
		concat(f_id, '_', EXTRACT(epoch FROM t)) AS f_id_sep
	FROM astra_t_prior WHERE t_prior >= {trk_split_interval} or t_prior IS NULL
), 
astra_sep AS (
	SELECT a.astra_id, a.t, a.x, a.y, a.gs, a.f_id, sep.f_id_sep
	FROM astra_sel AS a
	LEFT JOIN
	astra_sep_table AS sep
	ON (a.f_id=sep.f_id) AND (a.t>= sep.t_start) AND ((a.t<sep.t_end) OR sep.t_end IS NULL)
), 
astra_sep_locs AS (
	SELECT 
		a.t, 
		a.astra_id, 
		a.f_id, 
		a.f_id_sep, 
		a.x, 
		a.y, 
		a.gs, 
		CASE 
			WHEN gs >= {gs_air_threshold} AND array_agg(DISTINCT p.name) != '{empty_loc_array}'::text[] 
			THEN  array_agg(DISTINCT p.name) || 'air'::text 
			WHEN gs >= {gs_air_threshold} --AND
			THEN '{air_loc_array}'::text[]
			ELSE  array_agg(DISTINCT p.name) 
		END AS locs, -- array_agg(DISTINCT p.name) AS locs Extends locs with air when gs is large
		CASE 
			WHEN (array_agg(DISTINCT p.name)::text LIKE '{queue_polygon_pattern}') AND (a.gs <= {gs_queue_threshold}) 
			THEN (SELECT ARRAY(SELECT array_to_string(regexp_matches(array_agg(DISTINCT p.name)::text, '{queue_name_pattern}', 'g'),''))) 
		END AS self_queue
	FROM astra_sep AS a
	LEFT JOIN polygons AS p ON p.polygon @> point(a.x, a.y)
	GROUP BY a.t, a.astra_id, a.f_id, a.f_id_sep, a.x, a.y, a.gs
), 
astra_sep_locs_conn AS (
	SELECT a.t, a.astra_id, a.f_id, a.f_id_sep, a.x, a.y, a.gs, a.locs, a.self_queue, conn.astra_id AS conn_astra_id, conn.f_id_sep AS conn_f_id_sep, conn.self_queue AS conn_queue--,
	FROM astra_sep_locs AS a
	LEFT JOIN astra_sep_locs AS conn
	ON (POWER(a.x-conn.x,2) + POWER(a.y-conn.y,2) <= {distance_threshold}) AND a.gs<={gs_queue_threshold} AND conn.gs<={gs_queue_threshold} AND a.t = conn.t
), 
astra_sep_locs_conn_queue AS (
		SELECT a.t, a.astra_id, a.f_id, a.f_id_sep, a.x, a.y, a.gs, a.locs, a.self_queue, a.conn_astra_id, a.conn_f_id_sep, a.conn_queue--, a.queue
		FROM astra_sep_locs_conn as a
	UNION
		SELECT recursive_table.t, recursive_table.astra_id, recursive_table.f_id, recursive_table.f_id_sep, recursive_table.x, recursive_table.y, recursive_table.gs, recursive_table.locs, recursive_table.self_queue, matching_table.conn_astra_id, matching_table.conn_f_id_sep, matching_table.conn_queue--,
		FROM astra_sep_locs_conn AS matching_table
		JOIN astra_sep_locs_conn_queue AS recursive_table
		ON recursive_table.conn_astra_id = matching_table.astra_id AND recursive_table.t = matching_table.t
),  
astra_sep_locs_queue AS (
	SELECT t, astra_id, f_id, f_id_sep, x, y, gs, locs, array_set(array_accum(conn_queue)) AS queue 
	FROM astra_sep_locs_conn_queue 
	GROUP BY t, astra_id, f_id, f_id_sep, x, y, gs, locs
), 
astra_sep_prev_locs_queue AS ( -- CHANGED
	SELECT 
		*, 
		lag(locs) OVER (PARTITION BY f_id_sep ORDER BY t) AS prev_locs, 
		lag(queue) OVER (PARTITION BY f_id_sep ORDER BY t) AS prev_queue,
		COALESCE(
			EXTRACT(EPOCH FROM (t-lag(t) OVER (PARTITION BY f_id_sep ORDER BY t)))-1,
			0) AS s_last_contact 
	FROM astra_sep_locs_queue
), 
astra_sep_change_locs_queue AS (
	SELECT * 
	FROM astra_sep_prev_locs_queue
	WHERE (locs <> prev_locs) OR (prev_locs IS NULL) OR (queue <> prev_queue) OR (prev_queue IS NULL)
),
astra_sep_change_locs_queue_t_next AS (
	SELECT 
		*,
		lead(t) OVER(PARTITION BY f_id_sep ORDER BY t) AS t_next
	FROM astra_sep_change_locs_queue
), 
astra_sep_change_locs_queue_s_missed AS (
	SELECT
		a.t, 
		a.f_id, 
		a.f_id_sep, 
		a.locs, 
		a.queue, 
		a.x, 
		a.y, 
		a.gs,
		a.s_last_contact,
		SUM(b.s_last_contact) AS s_missed
	FROM
		astra_sep_change_locs_queue_t_next AS a
		LEFT JOIN
		astra_sep_prev_locs_queue AS b
		ON (a.f_id_sep = b.f_id_sep) AND (a.t <= b.t) AND COALESCE((a.t_next > b.t), TRUE)
	GROUP BY
	
		a.t, 
		a.f_id, 
		a.f_id_sep, 
		a.locs, 
		a.queue, 
		a.x, 
		a.y, 
		a.gs,
		a.s_last_contact
)
SELECT *
FROM astra_sep_change_locs_queue_s_missed
WHERE (t>=to_timestamp({start})) AND (t<to_timestamp({end})) 
ORDER BY t, astra_id
) WITH DATA;
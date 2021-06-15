-- CONTENT:
-- flt cleaning (moved to pandas)
-- flt_sep

-- flt cleaning
-- DONE IN PANDAS INSTEAD
-- -- df = df[ ~( (df['acid']==df['reg']) | ("N"+ df['acid']==df['reg']) ) ]
-- DELETE FROM flt_one_month WHERE (acid=reg) OR (concat('N', acid)=reg);

-- -- df = df[np.logical_xor(df['dest']=="EHAM", df['adep']=="EHAM")]
-- DELETE FROM flt_one_month WHERE (dest='EHAM' and adep='EHAM') or ( not dest='EHAM' and not adep='EHAM');

-- -- df = df[ ~(df['arrGnr'].str.contains("A[1-2]\d|GD|GL|HG.*").fillna(False)) & ~(df['depGnr'].str.contains("A[1-2]\d|GD|GL|HG.*").fillna(False)) ]
-- DELETE FROM flt_one_month WHERE  (arrgnr SIMILAR TO 'A[1-2]\d*|GD|GL|HG.*') OR (depgnr SIMILAR TO 'A[1-2]\d*|GD|GL|HG.*');

-- -- df = df[ df['actype'].isin(icao[icao["Description"]=="LandPlane"]["Type Designator"])]
-- DELETE FROM flt_one_month WHERE NOT actype IN (SELECT designator FROM icao WHERE description='LandPlane');

-- flt_sep
-- 40 min, 47min 15s
CREATE MATERIALIZED VIEW flt_processed AS (
	WITH flt_sel AS (
		SELECT 
			*, 
			CASE 
				WHEN adep='EHAM' AND dest!='EHAM' THEN 0 --dep
				WHEN adep!='EHAM' AND dest='EHAM' THEN 1 --arr
				WHEN adep!='EHAM' AND dest!='EHAM' THEN 2
				WHEN adep='EHAM' AND dest='EHAM' THEN 3
			END AS dep_arr
		FROM flt -- WHERE t >= '2018-01-03' AND  t < '2018-03-04'
	), flt_sep_first AS (
		SELECT sfplid, acid, t AS flt_sep_first 
		FROM (
			SELECT 
				*, 
				EXTRACT(epoch FROM t - lag(t) OVER (PARTITION BY sfplid, acid ORDER BY t ASC)) >= 8*60*60 OR
				EXTRACT(epoch FROM t - lag(t) OVER (PARTITION BY sfplid, acid ORDER BY t ASC)) IS NULL AS flt_sep_first 
			FROM flt_sel ORDER BY t
		) as flt_sel_first_column 
		WHERE  flt_sel_first_column.flt_sep_first
	), flt_sep_flown_last AS (
		SELECT 
			sfplid, 
			acid, 
			atd_ata,
			t AS flt_sep_last, 
			CONCAT(sfplid, '_', acid, '_', EXTRACT(epoch FROM atd_ata)) AS flt_sep_id
		FROM (
			SELECT ind, t, sfplid, acid, atd AS atd_ata, row_number() OVER (PARTITION BY sfplid, acid, atd ORDER BY t DESC) AS n_row
			FROM flt_sel
			WHERE (atd IS NOT NULL) AND (adep='EHAM')
			UNION
			SELECT ind, t, sfplid, acid, ata AS atd_ata, row_number() OVER (PARTITION BY sfplid, acid, ata ORDER BY t DESC) AS n_row
			FROM flt_sel
			WHERE (ata IS NOT NULL) AND (dest='EHAM')
		) AS a
		WHERE (n_row = 1)
	), flt_sep_flown_table AS (
		SELECT 
			flt_sep_last.sfplid, 
			flt_sep_last.acid, 
			flt_sep_last.atd_ata,
			max(flt_sep_first.flt_sep_first) AS flt_sep_first,
			flt_sep_last.flt_sep_last, 
			flt_sep_last.flt_sep_id
		FROM  
			flt_sep_flown_last AS flt_sep_last
			LEFT JOIN
			flt_sep_first AS flt_sep_first
			ON flt_sep_last.sfplid = flt_sep_first.sfplid AND flt_sep_last.acid = flt_sep_first.acid
		WHERE flt_sep_last.flt_sep_last >= flt_sep_first.flt_sep_first
		GROUP BY 
			flt_sep_last.sfplid,
			flt_sep_last.acid, 
			flt_sep_last.atd_ata,
			flt_sep_last.flt_sep_last, 
			flt_sep_last.flt_sep_id
	), flt_sep_flown AS (
		SELECT a.*, true as flown, b.flt_sep_id, b.flt_sep_first, b.flt_sep_last -- Both are inclusive times
		FROM flt_sel AS a
		LEFT JOIN flt_sep_flown_table AS b
		ON (a.sfplid=b.sfplid) AND (a.acid=b.acid) AND (a.t >= b.flt_sep_first) AND (a.t<= b.flt_sep_last) -- There should be no overlap so both inclusive
		WHERE flt_sep_id IS NOT NULL
	), flt_cancelled AS (
		SELECT a.* 
		FROM flt_sel AS a
		LEFT JOIN flt_sep_flown_table AS b
		ON (a.sfplid=b.sfplid) AND (a.acid=b.acid) AND (a.t >= b.flt_sep_first) AND (a.t<= b.flt_sep_last)
		WHERE flt_sep_id IS NULL
	), flt_sep_cancelled_table AS ( -- This section could be optimized to use flt_sep_first, but deemed unnecessary
		SELECT 
			flt_sep_start_table.acid, 
			flt_sep_start_table.sfplid, 
			flt_sep_start_table.flt_sep_start, 
			flt_sep_end_table.flt_sep_end, 
			CONCAT(flt_sep_start_table.sfplid, '_', flt_sep_start_table.acid, '_', EXTRACT( epoch FROM flt_sep_end_table.flt_sep_end), '_DNF') AS flt_sep_id
		FROM 
			(
				SELECT * 
				FROM 
					(
						-- acid sfplid t of cancelled flight entries with no equal acid sfplid entry in last 24 hours
						SELECT acid, sfplid, t AS flt_sep_start, row_number() OVER (PARTITION BY sfplid, acid ORDER BY t) AS n
						FROM (
							SELECT acid, sfplid, t,
								EXTRACT(epoch FROM (t - lag(t) OVER (PARTITION BY sfplid, acid ORDER BY t ASC))) >= 8*60*60 
								OR 
								EXTRACT(epoch FROM (t - lag(t) OVER (PARTITION BY sfplid, acid ORDER BY t ASC))) IS NULL AS flt_sep_start_bool
							FROM flt_cancelled
						) AS flt_sep_start_table_bool
						WHERE flt_sep_start_table_bool.flt_sep_start_bool
					) AS flt_sep_start_table_nrow
			) AS flt_sep_start_table
			INNER JOIN
			(
				SELECT *
				FROM
					(
						SELECT acid, sfplid, t AS flt_sep_end, row_number() OVER (PARTITION BY sfplid, acid ORDER BY t) AS n
						FROM (
							SELECT t, acid, sfplid, 
								EXTRACT(epoch FROM (lead(t) OVER (PARTITION BY sfplid, acid ORDER BY t ASC)-t)) >= 8*60*60 
								OR 
								EXTRACT(epoch FROM (lead(t) OVER (PARTITION BY sfplid, acid ORDER BY t ASC)-t)) IS NULL AS flt_sep_end_bool
							FROM flt_cancelled
						) AS flt_sep_end_table_bool
						WHERE flt_sep_end_table_bool.flt_sep_end_bool
					) AS flt_sep_end_table_nrow
				) AS flt_sep_end_table
			ON flt_sep_start_table.acid = flt_sep_end_table.acid AND flt_sep_start_table.sfplid = flt_sep_end_table.sfplid AND flt_sep_start_table.n = flt_sep_end_table.n
	), flt_sep_cancelled AS (
		SELECT a.*, false AS flown, b.flt_sep_id, b.flt_sep_start, b.flt_sep_end
		FROM
			flt_cancelled AS a
			LEFT JOIN
			flt_sep_cancelled_table AS b
			ON (a.sfplid=b.sfplid) AND (a.acid=b.acid) AND (a.t >= b.flt_sep_start) AND (a.t<= b.flt_sep_end)
	), flt_sep AS (
		SELECT *, -- t AS valid_from, lead(flt_sep.t) OVER (PARTITION BY flt_sep.flt_sep_id ORDER BY t ASC) AS valid_till
			CASE WHEN lead(flt_sep.t) OVER (PARTITION BY flt_sep.flt_sep_id ORDER BY t ASC) IS NULL THEN tstzrange(t, t) 
			ELSE tstzrange(t, lead(flt_sep.t) OVER (PARTITION BY flt_sep.flt_sep_id ORDER BY t ASC)) END AS valid_range
		FROM (
			SELECT *
			FROM flt_sep_flown
			UNION
			SELECT *
			FROM flt_sep_cancelled
		) AS flt_sep
	)
	SELECT * 
	FROM flt_sep 
) WITH DATA;

-- 40 min:

CREATE INDEX flt_processed_id ON flt_processed (flt_sep_id);
CREATE INDEX flt_processed_t ON flt_processed (t);
CREATE INDEX flt_processed_obt_predict ON flt_processed (obt_predict);
CREATE INDEX flt_processed_at_predict ON flt_processed (at_predict);
CREATE INDEX flt_processed_valid_range ON flt_processed USING gist (valid_range);


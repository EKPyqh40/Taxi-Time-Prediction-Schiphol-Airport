-- CONTENT:
-- * queue functions
-- * queue test data

-- queue functions
CREATE OR REPLACE FUNCTION array_set(ANYARRAY)
RETURNS ANYARRAY
LANGUAGE SQL
AS $$
SELECT ARRAY(
    SELECT DISTINCT $1[i]
    FROM generate_series(
        array_lower($1,1),
        array_upper($1,1)
    ) AS i
);
$$;

CREATE AGGREGATE array_accum (anyarray)
(
    sfunc = array_cat,
    stype = anyarray,
    initcond = '{}'
);  

-- queue test data
DROP  TABLE astra_queue_sample;
DROP TABLE polygons_queue_sample;

CREATE TABLE astra_queue_sample
(
    t bigint,
	astra_id bigserial,
	f_id text,
	f_id_sep text,
    x double precision,
    y double precision,
    gs double precision,
    CONSTRAINT astra_queue_pkey1 PRIMARY KEY (t, f_id)

);

INSERT INTO astra_queue_sample (f_id, f_id_sep, t, x, y, gs) VALUES
('AC_01', 'AC_01_00', 	0,	0, 		0, 		50),
('AC_02', 'AC_02_00', 	0,	0, 		150, 	1),
('AC_03', 'AC_03_00', 	0,	150,	150,	1),
('AC_04', 'AC_04_00', 	0,	450,	0, 		50),
('AC_05', 'AC_05_00', 	0,	450,	150, 	1),
('AC_06', 'AC_06_00', 	0,	450,	300, 	1),
('AC_01', 'AC_01_00',  	1,	450,	300, 	1),
('AC_02', 'AC_02_00',  	1,	450,	450, 	1),
('AC_03', 'AC_03_00',  	1,	450,	600, 	1),
('AC_04', 'AC_04_00',  	1,	450,	750, 	1),
('AC_05', 'AC_05_00',  	1,	450,	900, 	50),
('AC_06', 'AC_06_00',  	1,	450,	1050, 	1),
('AC_07', 'AC_07_01',  	1,	600,	900, 	1),
('AC_08', 'AC_08_01',  	1,	600,	600, 	1),
('AC_09', 'AC_09_01',  	1,	675,	525, 	1),
('AC_10', 'AC_10_01', 	1,	750,	600, 	1),
('AC_11', 'AC_11_01', 	1,	1050,   600, 	1),
('AC_12', 'AC_12_01', 	1,	1200,   600, 	1),
('AC_13', 'AC_13_01', 	1,	600, 	1050, 	1),
('AC_14', 'AC_14_01', 	1,	750, 	1050,	1);

CREATE TABLE polygons_queue_sample
(
    index SERIAL,
    name text,
    polygon polygon,
    CONSTRAINT polygons_pkey1 PRIMARY KEY (index)
);

INSERT INTO polygons_queue_sample (name, polygon) VALUES
('04/22_threshold', '((375,225),(375,375),(525,375),(525,225))'),
('ramp', 			'((-75,-75),(-75,225),(75,225),(75,-75))'),
('left', 			'((0,0),(0,750),(300,750),(300,0))'),
('36L_threshold', 	'((375, 975), (375, 1125), (525, 1125), (525, 975))'),
('36R_threshold', 	'((675,975), (675,1125), (825,1125), (825,975))'),
('36C_threshold',	'((375, 975), (375, 1125), (825,1125), (825,975))');
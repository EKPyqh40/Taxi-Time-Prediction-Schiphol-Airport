DROP TABLE icao;

CREATE TABLE icao (
	manufacturer text,
	model text,
	designator text,
	description	text,
	engine_type text,
	engine_count text,
	wtc text
);

CREATE INDEX icao_designator_index
	ON icao USING btree
	(designator ASC NULLS last)
;

"C:\\Program Files\\PostgreSQL\\13\\bin\\psql.exe" --command " "\\copy public.icao (manufacturer, model, \"Type Designator\", description, \"Engine Type\", \"Engine Count\", wtc) FROM 'C:/Users/cvaka/OneDrive/Master/Thesis/Data/ICAO_D~1.CSV' DELIMITER ',' CSV HEADER QUOTE '\"' ESCAPE '''';""

ALTER TABLE icao 
DROP COLUMN "manufacturer", 
DROP COLUMN "model", 
DROP COLUMN "engine_type", 
DROP COLUMN "engine_count";


-- Drop Duplicate Designators
ALTER TABLE icao 
ADD COLUMN id bigserial;

DELETE FROM
    icao AS a
        USING icao AS b
WHERE
    a.id < b.id
    AND a.designator= b.designator;

ALTER TABLE icao
DROP COLUMN id,
ADD PRIMARY KEY (designator);
CREATE TABLE astra 
(
    astra_id bigserial,
    t timestamp with time zone,
    trk bigint,
    ssr bigint,
    x double precision,
    y double precision,
    fl bigint,
    gs bigint,
    heading bigint,
    f_id text NOT NULL,
    "mode-s" bigint,
    PRIMARY KEY (t, f_id)
);

CREATE INDEX t
    ON astra USING btree
    (t ASC NULLS LAST)
;

CREATE INDEX astra_id
    ON astra USING btree
    (astra_id ASC NULLS LAST)
;
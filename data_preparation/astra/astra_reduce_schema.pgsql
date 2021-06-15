CREATE TABLE astra_processed (
    astra_id bigint, -- Should be removed in next update
    t timestamp with time zone,
    f_id text,
    f_id_sep text,
    locs text[],
    queue text[],
    x bigint,
    y bigint,
    gs bigint,
    s_last_contact bigint,
    s_missed bigint,
    PRIMARY KEY (t, f_id_sep)
);

CREATE INDEX t_astra_processed
    ON astra_processed USING btree
    (t ASC NULLS LAST);

CREATE INDEX f_id_sep_astra_processed
    ON astra_processed USING btree
    (f_id_sep ASC NULLS LAST);
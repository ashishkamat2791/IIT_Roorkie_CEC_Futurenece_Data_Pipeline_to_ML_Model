-- Append one source file to its raw table exactly as received.
-- all_varchar + a null string that never occurs keeps blanks as '' and keeps whitespace,
-- so raw values are byte-for-byte the CSV values. The header was already checked against
-- the column contract in Python, and BY NAME maps columns by name, never by position.
-- A retry of the same run first removes that run's own rows, so retries never duplicate;
-- rows from earlier runs are never touched.
DELETE FROM {{ table }} WHERE _run_id = {{ run_id }} AND _source_file = {{ source_file }};

INSERT INTO {{ table }} BY NAME
SELECT src.*,
       row_number() OVER () AS _row_num,
       {{ source_file }}    AS _source_file,
       CAST({{ loaded_at }} AS TIMESTAMP) AS _loaded_at,
       {{ run_id }}         AS _run_id,
       {{ file_md5 }}       AS _file_md5
FROM read_csv({{ path }}, header = true, all_varchar = true, delim = ',', quote = '"',
              escape = '"', nullstr = '\x01__never_null__', strict_mode = true) AS src;

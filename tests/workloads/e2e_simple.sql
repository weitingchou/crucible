-- @type: mysql

-- @name: CountAll
SELECT COUNT(*) AS cnt FROM test_data;

-- @name: SelectById
SELECT id, value FROM test_data WHERE id = 1;

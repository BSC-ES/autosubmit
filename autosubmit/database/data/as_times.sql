CREATE TABLE experiment_status(
	exp_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	status TEXT NOT NULL,
	seconds_diff INTEGER NOT NULL,
    modified TEXT NOT NULL,
    last_heartbeat TEXT);
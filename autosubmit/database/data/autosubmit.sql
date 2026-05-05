CREATE TABLE experiment(
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	name VARCHAR NOT NULL,
	description VARCHAR NOT NULL,
	autosubmit_version VARCHAR);
CREATE TABLE db_version(
    version INTEGER NOT NULL);
INSERT INTO db_version (version) VALUES (1);
CREATE TABLE details(
	exp_id INTEGER NOT NULL,
	user TEXT NOT NULL,
	created TEXT NOT NULL,
	model TEXT NOT NULL,
	branch TEXT NOT NULL,
	hpc TEXT NOT NULL);

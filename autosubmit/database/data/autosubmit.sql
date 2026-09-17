CREATE TABLE experiment(
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	name VARCHAR NOT NULL,
	description VARCHAR NOT NULL,
	autosubmit_version VARCHAR);
CREATE TABLE db_version(
    version INTEGER NOT NULL);
INSERT INTO db_version (version) VALUES (1);
CREATE TABLE general_schema_migrations(
    version INTEGER NOT NULL PRIMARY KEY,
    applied_at TEXT NOT NULL);
INSERT INTO general_schema_migrations (version, applied_at) VALUES (1, datetime('now'));
CREATE DATABASE superset_metadata;
CREATE USER superset_user WITH ENCRYPTED PASSWORD 'superset_pass';
GRANT ALL PRIVILEGES ON DATABASE superset_metadata TO superset_user;


\c superset_metadata

GRANT CREATE ON SCHEMA public TO superset_user;
GRANT USAGE ON SCHEMA public TO superset_user;
ALTER SCHEMA public OWNER TO superset_user;

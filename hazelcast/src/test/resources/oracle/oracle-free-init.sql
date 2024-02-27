-- This script is executed by system user
-- Grant all the privileges to testcontainers user, so that the user can create schemas
-- See OracleSchemaJdbcSqlConnectorTest for schema tests
ALTER SESSION SET CONTAINER=FREEPDB1;
GRANT ALL PRIVILEGES TO test;
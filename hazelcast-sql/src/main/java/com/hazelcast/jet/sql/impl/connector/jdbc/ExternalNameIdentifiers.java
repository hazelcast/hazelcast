package com.hazelcast.jet.sql.impl.connector.jdbc;

public class ExternalNameIdentifiers {

    private String catalog;

    private String schemaName;

    private String tableName;

    public String getCatalog() {
        return catalog;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void parseExternalTableName(String externalTableName) {
        //"schema.with.dot.in.name"."table.with.dot.in.name"
        if (externalTableName.contains("\".\"")) {
            splitByQuotedDot(externalTableName);
        } else {
            splitByDot(externalTableName);
        }
    }

    void splitByQuotedDot(String externalTableName) {
        String[] split = externalTableName.split("\"\\.\"");
        if (split.length == 3) {
            catalog = split[0];
            schemaName = split[1];
            tableName = split[2];
        } else if (split.length == 2) {
            schemaName = split[0];
            tableName = split[1];
        } else {
            tableName = split[0];
        }
    }

    void splitByDot(String externalTableName) {
        String[] split = externalTableName.split("\\.");
        if (split.length == 3) {
            catalog = split[0];
            schemaName = split[1];
            tableName = split[2];
        } else if (split.length == 2) {
            schemaName = split[0];
            tableName = split[1];
        } else {
            tableName = split[0];
        }
    }
}

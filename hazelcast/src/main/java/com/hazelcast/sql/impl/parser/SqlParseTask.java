package com.hazelcast.sql.impl.parser;

import com.hazelcast.sql.impl.schema.Catalog;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the parse task.
 */
public final class SqlParseTask {

    /**
     * The query.
     */
    private final String sql;

    /**
     * Catalog.
     */
    private final Catalog catalog;

    /**
     * The scopes for object lookup in addition to the default ones.
     */
    private final List<List<String>> searchPaths;

    private SqlParseTask(String sql, Catalog catalog, List<List<String>> searchPaths) {
        this.sql = sql;
        this.catalog = catalog;
        this.searchPaths = searchPaths;
    }

    public String getSql() {
        return sql;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public List<List<String>> getSearchPaths() {
        return searchPaths;
    }

    public static class Builder {

        private final String sql;
        private final Catalog catalog;
        private List<List<String>> searchPaths;

        public Builder(String sql, Catalog catalog) {
            this.sql = sql;
            this.catalog = catalog;
        }

        public Builder addSchemaPath(List<String> searchPath) {
            if (searchPaths == null) {
                searchPaths = new ArrayList<>(1);
            }

            searchPaths.add(searchPath);

            return this;
        }

        public SqlParseTask build() {
            return new SqlParseTask(sql, catalog, searchPaths);
        }
    }
}

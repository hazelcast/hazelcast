/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SQL query.
 */
public class SqlQuery {
    /** Default page size. */
    public static final int DEFAULT_PAGE_SIZE = 1000;

    /** SQL query. */
    private String sql;

    /** Parameters. */
    private List<Object> parameters;

    /** Timeout. */
    // TODO: Support timeout in executor.
    private long timeout;

    /** Page size. */
    // TODO: Support page size in executor.
    private int pageSize = DEFAULT_PAGE_SIZE;

    /** Maximum number of rows to fetch. */
    // TODO: Support max rows in executor.
    private int maxRows;

    public SqlQuery(String sql) {
        setSql(sql);
    }

    public String getSql() {
        return sql;
    }

    public SqlQuery setSql(String sql) {
        if (sql == null || sql.length() == 0) {
            throw new IllegalArgumentException("SQL cannot be null or empty.");
        }

        this.sql = sql;

        return this;
    }

    public List<Object> getParameters() {
        return parameters != null ? parameters : Collections.emptyList();
    }

    public SqlQuery setParameters(List<Object> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            this.parameters = null;
        } else {
            this.parameters = new ArrayList<>(parameters);
        }

        return this;
    }

    public SqlQuery addParameter(Object parameter) {
        if (parameters == null) {
            parameters = new ArrayList<>(1);
        }

        parameters.add(parameter);

        return this;
    }

    public SqlQuery clearParameters() {
        this.parameters = null;

        return this;
    }

    public int getPageSize() {
        return pageSize;
    }

    public SqlQuery setPageSize(int pageSize) {
        if (pageSize < 0) {
            throw new IllegalArgumentException("Page size cannot be negative: " + pageSize);
        }

        this.pageSize = pageSize;

        return this;
    }

    public long getTimeout() {
        return timeout;
    }

    public SqlQuery setTimeout(long timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout cannot be negative: " + timeout);
        }

        this.timeout = timeout;

        return this;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public SqlQuery setMaxRows(int maxRows) {
        if (maxRows < 0) {
            throw new IllegalArgumentException("Max rows cannot be negative: " + maxRows);
        }

        this.maxRows = maxRows;

        return this;
    }
}

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
 * Definition of the SQL query.
 */
public class SqlQuery {
    /** Default page size. */
    public static final int DEFAULT_PAGE_SIZE = 1000;

    /** SQL query. */
    private String sql;

    /** Parameters. */
    private List<Object> parameters;

    /** Timeout. */
    private long timeout;

    /** Page size. */
    private int pageSize = DEFAULT_PAGE_SIZE;

    public SqlQuery() {
        // No-op.
    }

    public SqlQuery(String sql) {
        setSql(sql);
    }

    /**
     * Copying constructor.
     */
    private SqlQuery(String sql, List<Object> parameters, long timeout, int pageSize) {
        setSql(sql);
        setParameters(parameters);
        setTimeout(timeout);
        setPageSize(pageSize);
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

    public long getTimeout() {
        return timeout;
    }

    /**
     * The query timeout in milliseconds or 0 for no timeout.
     */
    public SqlQuery setTimeout(long timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout cannot be negative: " + timeout);
        }

        this.timeout = timeout;

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

    public SqlQuery copy() {
        return new SqlQuery(sql, parameters, timeout, pageSize);
    }
}

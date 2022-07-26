/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.impl;

import java.util.Objects;

public class SqlSummary {
    private String query;
    private boolean unbounded;

    public SqlSummary() {
    }

    public SqlSummary(String query, boolean unbounded) {
        this.query = query;
        this.unbounded = unbounded;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public boolean isUnbounded() {
        return unbounded;
    }

    public void setUnbounded(boolean unbounded) {
        this.unbounded = unbounded;
    }

    @Override
    public String toString() {
        return "SqlSummary{" +
                "query='" + query + '\'' +
                ", unbounded=" + unbounded +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlSummary that = (SqlSummary) o;
        return unbounded == that.unbounded && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, unbounded);
    }
}

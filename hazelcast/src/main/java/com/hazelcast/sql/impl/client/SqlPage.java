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

package com.hazelcast.sql.impl.client;

import com.hazelcast.internal.serialization.Data;

import java.util.List;

public class SqlPage {

    private final List<Data> rows;
    private final boolean last;

    public SqlPage(List<Data> rows, boolean last) {
        this.rows = rows;
        this.last = last;
    }

    public List<Data> getRows() {
        return rows;
    }

    public boolean isLast() {
        return last;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlPage page = (SqlPage) o;

        if (last != page.last) {
            return false;
        }

        return rows != null ? rows.equals(page.rows) : page.rows == null;
    }

    @Override
    public int hashCode() {
        int result = rows != null ? rows.hashCode() : 0;

        result = 31 * result + (last ? 1 : 0);

        return result;
    }
}

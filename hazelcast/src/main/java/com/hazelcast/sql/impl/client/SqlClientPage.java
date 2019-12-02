/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.SqlRow;

import java.util.List;

/**
 * Single page which is going to be supplied to the client.
 */
public class SqlClientPage {
    /** Rows. */
    private final List<SqlRow> rows;

    /** Whether this is the last page. */
    private final boolean last;

    public SqlClientPage(List<SqlRow> rows, boolean last) {
        this.rows = rows;
        this.last = last;
    }

    public List<SqlRow> getRows() {
        return rows;
    }

    public boolean isLast() {
        return last;
    }
}

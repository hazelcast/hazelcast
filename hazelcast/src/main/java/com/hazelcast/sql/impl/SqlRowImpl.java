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


package com.hazelcast.sql.impl;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.row.Row;

/**
 * Default implementation of the SQL row which is exposed to users. We merely wrap the internal row, but add more checks which
 * is important for user-facing code, but which could cause performance degradation if implemented in the internal classes.
 */
public class SqlRowImpl implements SqlRow {

    private final Row row;
    private final int columnCount;

    public SqlRowImpl(Row row) {
        this.row = row;

        columnCount = row.getColumnCount();
    }

    @Override
    public <T> T getObject(int index) {
        checkIndex(index);

        return row.get(index);
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= columnCount) {
            throw new IllegalArgumentException("Column index is out of range: " + index);
        }
    }

    public Row getDelegate() {
        return row;
    }
}

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

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;

public class SingleValueCursor implements SqlCursor {

    private final SqlColumnMetadata metadata;
    private Iterator<SqlRow> iterator;

    public <T> SingleValueCursor(T value) {
        QueryDataType queryDataType = QueryDataTypeUtils.resolveTypeForClass(value.getClass());
        this.metadata = QueryUtils.getColumnMetadata(queryDataType);

        SqlRow row = new SqlRowImpl(new HeapRow(new Object[]{value}));
        this.iterator = Collections.singleton(row).iterator();
    }

    @Override
    public int getColumnCount() {
        return 1;
    }

    @Override
    public SqlColumnMetadata getColumnMetadata(int index) {
        if (index != 0) {
            throw new IllegalArgumentException("Column index is out of range: " + index);
        }
        return metadata;
    }

    @Override
    @Nonnull
    public Iterator<SqlRow> iterator() {
        if (iterator == null) {
            throw QueryException.error("Iterator can be requested only once.");
        } else {
            Iterator<SqlRow> iterator = this.iterator;
            this.iterator = null;
            return iterator;
        }
    }

    @Override
    public void close() {
    }
}

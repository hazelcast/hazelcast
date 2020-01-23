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

import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * Cursor implementation.
 */
public class SqlCursorImpl implements SqlCursor {
    /** Handle. */
    private final QueryHandle handle;

    public SqlCursorImpl(QueryHandle handle) {
        this.handle = handle;
    }

    @Override @Nonnull
    public Iterator<SqlRow> iterator() {
        return handle.getConsumer().iterator();
    }

    @Override
    public void close() {
        handle.close();
    }

    public QueryHandle getHandle() {
        return handle;
    }
}

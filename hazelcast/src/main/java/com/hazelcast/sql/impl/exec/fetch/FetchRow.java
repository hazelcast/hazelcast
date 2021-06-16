/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.fetch;

import com.hazelcast.sql.impl.row.Row;

/**
 * Special implementation of row which is used for LIMIT/OFFSET expressions.
 * These expressions cannot refer to real columns, which is checked during query planning phase.
 */
public final class FetchRow implements Row {

    public static final FetchRow INSTANCE = new FetchRow();

    private FetchRow() {
        // No-op.
    }

    @Override
    public <T> T get(int index) {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public int getColumnCount() {
        throw new UnsupportedOperationException("Should not be called.");
    }
}

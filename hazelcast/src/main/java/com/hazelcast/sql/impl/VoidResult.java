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

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlResultType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import javax.annotation.Nonnull;
import java.util.Iterator;

public class VoidResult implements SqlResult {

    @Override @Nonnull
    public SqlResultType getResultType() {
        return SqlResultType.VOID;
    }

    @Override @Nonnull
    public SqlRowMetadata getRowMetadata() {
        throw noRowsResultExc();
    }

    @Override @Nonnull
    public Iterator<SqlRow> iterator() {
        throw noRowsResultExc();
    }

    private IllegalStateException noRowsResultExc() {
        throw new IllegalStateException("Not a " + SqlResultType.ROWS.name() + " result");
    }

    @Override
    public void close() {
        // No-op.
    }
}

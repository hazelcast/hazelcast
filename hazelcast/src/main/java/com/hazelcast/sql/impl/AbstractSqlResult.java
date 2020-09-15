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
import com.hazelcast.sql.SqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class AbstractSqlResult implements SqlResult {

    public abstract QueryId getQueryId();

    /**
     * Closes the result, releasing all the resources.
     *
     * @param exception exception that caused the close operation or {@code null} if the query is closed due to user request
     */
    public abstract void close(@Nullable QueryException exception);

    @Nonnull @Override
    public abstract ResultIterator<SqlRow> iterator();

    @Override
    public void close() {
        close(null);
    }
}

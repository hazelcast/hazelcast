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
import java.util.Iterator;

public abstract class AbstractSqlResult implements SqlResult {

    public abstract QueryId getQueryId();

    public abstract void closeOnError(QueryException exception);

    @Nonnull @Override
    public abstract ResultIterator<SqlRow> iterator();

    @Override
    public void close() {
        closeOnError(QueryException.cancelledByUser());
    }

    public interface ResultIterator<T> extends Iterator<T> {

        /**
         * A result value from {@link #hasNextImmediately()} meaning that a next
         * item is available immediately.
         */
        int YES = 2;

        /**
         * A result value from {@link #hasNextImmediately()} meaning that a next
         * item is not available immediately, but might be available later. The
         * caller should check again later.
         */
        int RETRY = 1;

        /**
         * A result value from {@link #hasNextImmediately()} meaning that the last
         * item was already returned. A call to {@link #next()} will fail.
         */
        int DONE = 0;

        /**
         * Checks if a next item is available immediately.
         *
         * @return One of {@link #YES}, {@link #RETRY} or {@link #DONE}
         */
        int hasNextImmediately();
    }
}

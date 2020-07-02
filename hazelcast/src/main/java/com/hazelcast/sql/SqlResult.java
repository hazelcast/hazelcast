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

package com.hazelcast.sql;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * SQL query result. Represents a stream of rows.
 * <p>
 * Use {@link #iterator()} to iterate over the rows. The iterator can be requested only once.
 * <p>
 * Use {@link #close()} to release the resources associated with the result.
 * <p>
 * Typical usage pattern:
 * <pre>
 * try (SqlResult result = hazelcastInstance.getSql().query("SELECT ...")) {
 *     for (SqlRow row : result) {
 *         // Process the row.
 *     }
 * }
 * </pre>
 *
 * @see #iterator()
 * @see #close()
 */
public interface SqlResult extends Iterable<SqlRow>, AutoCloseable {
    /**
     * Gets row metadata.
     *
     * @return row metadata
     */
    @Nonnull
    SqlRowMetadata getRowMetadata();

    /**
     * Returns the iterator over the result rows.
     * <p>
     * The iterator may be requested only once.
     *
     * @return iterator
     * @throws IllegalStateException if the method is invoked more than once
     * @throws SqlException in case of an SQL-related error condition
     */
    @Nonnull
    @Override
    Iterator<SqlRow> iterator();

    /**
     * Release the resources associated with the query result.
     * <p>
     * The query engine delivers the results asynchronously. The query may become inactive even before all rows are
     * consumed. The invocation of this command will cancel the execution of the query on all members if the query
     * is still active. Otherwise it is no-op.
     */
    @Override
    void close();
}

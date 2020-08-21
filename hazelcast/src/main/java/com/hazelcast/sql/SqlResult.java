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
 * SQL query result. Depending on the statement type it represents a stream of
 * rows or an update count.
 * <p>
 * <h4>Usage for a stream of rows</h4>
 *
 * <ol>
 *     <li>Use {@link #iterator()} to iterate over the rows.
 *     <li>Use {@link #close()} to release the resources associated with the
 *     result.
 * </ol>

 * <p>
 * Code example:
 * <pre>
 * try (SqlResult result = hazelcastInstance.getSql().execute("SELECT ...")) {
 *     for (SqlRow row : result) {
 *         // Process the row.
 *     }
 * }
 * </pre>
 *
 * <p>
 * <h4>Usage for update count</h4>
 *
 * <pre>
 *     long updated = hazelcastInstance.getSql().execute("UPDATE ...").updateCount();
 * </pre>
 *
 * You don't need to call {@link #close()} in this case.
 */
public interface SqlResult extends Iterable<SqlRow>, AutoCloseable {

    /**
     * If this result represents a row set, this method returns {@code false}.
     * If this result represents an update count (such as for a DML query), it
     * returns {@code true}.
     *
     * @return {@code false} for a rows result and {@code true} for an update
     *     count result
     */
    boolean isUpdateCount();

    /**
     * Gets row metadata.
     *
     * @throws IllegalStateException if this result doesn't have rows (i.e.
     *     when {@link #isUpdateCount()} returns {@code true})
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
     * @throws IllegalStateException if the method is invoked more than once or
     *    if this result doesn't have rows (i.e. when {@link #isUpdateCount()}
     *    returns {@code true})
     * @throws HazelcastSqlException in case of an SQL-related error condition
     */
    @Nonnull
    @Override
    Iterator<SqlRow> iterator();

    /**
     * Returns the number of rows updated by the statement.
     *
     * @throws IllegalStateException if this result doesn't represent an update
     *     count (i.e. when {@link #isUpdateCount()} returns {@code false})
     */
    long updateCount();

    /**
     * Release the resources associated with the query result. Must be called only if the {@linkplain
     * #isUpdateCount()} returns {@code false}, that is when it's a result with rows, otherwise it's a no-op.
     * <p>
     * The query engine delivers the rows asynchronously. The query may become inactive even before all rows are
     * consumed. The invocation of this command will cancel the execution of the query on all members if the query
     * is still active. Otherwise it is no-op.
     */
    @Override
    void close();
}

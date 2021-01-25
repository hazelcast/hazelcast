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
 * A handle to SQL statement result. Depending on the statement type it
 * represents a stream of rows or an update count.
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
 * <h4>Usage for update count</h4>
 *
 * <pre>
 *     long updated = hazelcastInstance.getSql().execute("UPDATE ...").updateCount();
 * </pre>
 *
 * Make sure you always call the {@code updateCount()} method, even if you
 * don't need the update count. The {@code SqlResult} object is created
 * before the statement is executed, without calling {@code updateCount()}
 * you might miss the execution error and you won't know when the statement
 * actually completed.
 * <p>
 * You don't need to call {@link #close()} in case of a result with an
 * update count.
 */
public interface SqlResult extends Iterable<SqlRow>, AutoCloseable {

    /**
     * Return whether this result has rows to iterate using the {@link
     * #iterator()} method.
     *
     * @throws HazelcastSqlException in case of an execution error
     */
    default boolean isRowSet() {
        return updateCount() == -1;
    }

    /**
     * Gets the row metadata.
     *
     * @throws HazelcastSqlException in case of an execution error
     * @throws IllegalStateException if the result doesn't have rows, but
     *     only an update count
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
     *    if this result doesn't have rows
     * @throws HazelcastSqlException in case of an SQL-related error condition.
     *    This error can also be thrown when iterating the returned iterator.
     */
    @Nonnull
    @Override
    Iterator<SqlRow> iterator();

    /**
     * Returns the number of rows updated by the statement or -1 if this result
     * is a row set. In case the result doesn't contain rows but the update
     * count isn't applicable or known, 0 is returned.
     * <p>
     * If this result is a result with an update count, the call will block
     * until the statement completes. Make sure to always call this method even
     * if you don't need the update count or even if there's no actual update
     * count (as in the case of Jet's DDL statements where it's always 0). The
     * {@link SqlResult} object is a handle created before the statement is
     * executed. Most run-time error will be thrown from this method.
     *
     * @throws HazelcastSqlException in case of an execution error
     */
    long updateCount();

    /**
     * Release the resources associated with the query result.
     * <p>
     * The query engine delivers the rows asynchronously. The query may become inactive even before all rows are
     * consumed. The invocation of this command will cancel the execution of the query on all members if the query
     * is still active. Otherwise it is no-op. For a result with an update count it is always no-op.
     */
    @Override
    void close();
}

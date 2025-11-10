/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
     * Return whether this result has rows to iterate using the {@link
     * #iterator()} method.
     */
    default boolean isRowSet() {
        return updateCount() == -1;
    }

    /**
     * Gets the row metadata.
     *
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
     * @throws HazelcastSqlException in case of an SQL-related error condition
     */
    @Nonnull
    @Override
    Iterator<SqlRow> iterator();

    /**
     * Returns a stream of result rows.
     * <p>It uses internally {@link #iterator()} method, so it cannot be called twice.</p>
     *
     * <p>You should still call {@link #close()} method after the stream is used (or use this method inside
     * {@code try-with-resources} block. You should not pass the {@link Stream} from this method outside
     * {@code try-with-resources} block, if it's used.</p>
     *
     * @throws IllegalStateException if the method is invoked more than once or
     *    if this result doesn't have rows
     * @throws HazelcastSqlException in case of an SQL-related error condition
     *
     * @return Stream of result rows
     *
     * @since 5.4
     */
    @Nonnull
    default Stream<SqlRow> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Returns the number of rows updated by the statement or -1 if this result
     * is a row set. In case the result doesn't contain rows but the update
     * count isn't applicable or known, 0 is returned.
     *
     * @deprecated this method always returns {@code 0} for DML statements and should not be used.
     */
    @Deprecated(since = "5.6", forRemoval = true)
    long updateCount();

    /**
     * Release the resources associated with the query result.
     * <p>
     * The query engine delivers the rows asynchronously. The query may become inactive even before all rows are
     * consumed. The invocation of this command will cancel the execution of the query on all members if the query
     * is still active. Otherwise, it is no-op. For a result with an update count it is always no-op.
     */
    @Override
    void close();
}

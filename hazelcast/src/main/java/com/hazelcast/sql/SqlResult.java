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
 * rows or a void result.
 * <p>
 * <h4>Usage for {@link SqlResultType#ROWS ROWS} {@linkplain #getResultType()
 * result type}</h4>
 *
 * <ol>
 *     <li>Use {@link #iterator()} to iterate over the rows. The iterator can
 *     be requested only once.

 *     <li>Use {@link #close()} to release the resources associated with the
 *     result.
 * </ol>

 * <p>
 * Code example:
 * <pre>
 * try (SqlResult result = hazelcastInstance.getSql().query("SELECT ...")) {
 *     for (SqlRow row : result) {
 *         // Process the row.
 *     }
 * }
 * </pre>
 *
 * <p>
 * <h4>Usage for {@link SqlResultType#VOID VOID} {@linkplain #getResultType()
 * result type}</h4>
 *
 * <pre>
 *     hazelcastInstance.getSql().query("CREATE ...");
 * </pre>
 */
public interface SqlResult extends Iterable<SqlRow>, AutoCloseable {
    /**
     * Gets row metadata.
     *
     * @throws IllegalStateException if the {@linkplain #getResultType() result
     *      type} isn't {@link SqlResultType#ROWS ROWS}.
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
     *      if the {@linkplain #getResultType() result type} isn't {@link
     *      SqlResultType#ROWS ROWS}.
     * @throws SqlException in case of an SQL-related error condition
     */
    @Nonnull
    @Override
    Iterator<SqlRow> iterator();

    /**
     * Returns the result type.
     */
    @Nonnull
    SqlResultType getResultType();

    /**
     * Release the resources associated with the query result. Only must be called if the {@linkplain
     * #getResultType() result type} is {@link SqlResultType#ROWS ROWS}, for other result types it's a no-op.
     * <p>
     * The query engine delivers the rows asynchronously. The query may become inactive even before all rows are
     * consumed. The invocation of this command will cancel the execution of the query on all members if the query
     * is still active. Otherwise it is no-op.
     */
    @Override
    void close();
}

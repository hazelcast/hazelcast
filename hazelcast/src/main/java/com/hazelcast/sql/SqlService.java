/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import java.util.List;

/**
 * A service to execute SQL statements.
 * <p>
 * In order to use the service, Jet engine must be enabled - SQL statements are executed as
 * Jet jobs. On members, the {@code hazelcast-sql.jar} must be on the classpath, otherwise
 * an exception will be thrown; on client, it is not necessary.
 * <p>
 * <h1>Overview</h1>
 * Hazelcast is currently able to execute distributed SQL queries using the following connectors:
 * <ul>
 *     <li>IMap
 *     <li>Kafka
 *     <li>Files
 * </ul>
 * When an SQL statement is submitted to a member, it is parsed and optimized by the {@code hazelcast-sql} module,
 * that is based on <a href="https://calcite.apache.org">Apache Calcite</a>. During optimization a statement is
 * converted into a directed acyclic graph (DAG) that is sent to cluster members for execution. Results are sent
 * back to the originating member asynchronously and returned to the user via {@link SqlResult}.
 * <p>
 * SQL statements are not atomic. <em>INSERT</em>/<em>SINK</em> can fail and commit part of the data.
 * <p>
 * <h1>Usage</h1>
 * Before you can access any object using SQL, a <em>mapping</em> has to be created. See the reference manual for the
 * <em>CREATE MAPPING</em> command.
 * <p>
 * When a query is executed, an {@link SqlResult} is returned. You may get row iterator from the result. The result must
 * be closed at the end. The code snippet below demonstrates a typical usage pattern:
 * <pre>
 *     HazelcastInstance instance = ...;
 *
 *     try (SqlResult result = instance.sql().execute("SELECT * FROM person")) {
 *         for (SqlRow row : result) {
 *             long personId = row.getObject("personId");
 *             String name = row.getObject("name");
 *             ...
 *         }
 *     }
 * </pre>
 */
public interface SqlService {
    /**
     * Convenient method to execute a distributed query with the given
     * parameter values.
     * <p>
     * Converts passed SQL string and parameter values into an {@link
     * SqlStatement} object and invokes {@link #execute(SqlStatement)}.
     *
     * @param sql       SQL string
     * @param arguments query parameter values that will be passed to {@link SqlStatement#setParameters(List)}
     * @return result
     * @throws NullPointerException     if the SQL string is null
     * @throws IllegalArgumentException if the SQL string is empty
     * @throws HazelcastSqlException    in case of execution error
     * @see SqlService
     * @see SqlStatement
     * @see #execute(SqlStatement)
     */
    @Nonnull
    default SqlResult execute(@Nonnull String sql, Object... arguments) {
        SqlStatement statement = new SqlStatement(sql);

        if (arguments != null) {
            for (Object arg : arguments) {
                statement.addParameter(arg);
            }
        }

        return execute(statement);
    }

    /**
     * Executes an SQL statement.
     *
     * @param statement statement to be executed
     * @return result
     * @throws NullPointerException  if the statement is null
     * @throws HazelcastSqlException in case of execution error
     * @see SqlService
     */
    @Nonnull
    SqlResult execute(@Nonnull SqlStatement statement);
}

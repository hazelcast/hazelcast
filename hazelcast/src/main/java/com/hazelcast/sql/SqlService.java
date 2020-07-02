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

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Service to execute distributed SQL queries.
 * <p>
 * The service is in beta state. Behavior and API might be changed in future releases. Binary compatibility is not
 * guaranteed between minor and patch releases.
 * <h1>Overview</h1>
 * Hazelcast is able to execute distributed SQL queries over the following entities:
 * <ul>
 *     <li>IMap
 * </ul>
 * When a query is submitted to a member, it is parsed and optimized by the {@code hazelcast-sql} module, that is based on
 * <a href="https://calcite.apache.org">Apache Calcite</a>. The {@code hazelcast-sql} must be in the classpath, otherwise
 * an exception will be thrown.
 * <p>
 * During optimization a query is converted into a directed acyclic graph (DAG) that is sent to cluster members for execution.
 * Query results are sent back to the originating member asynchronously and returned to the user via {@link SqlResult}.
 *
 * <h1>Querying an IMap</h1>
 * Every IMap instance is exposed as a table with the same name in the {@code partitioned} schema. The {@code partitioned}
 * schema is included into a default search path, therefore an IMap could be referenced in an SQL query with or without the
 * schema name.
 * <h2>Column resolution</h2>
 * Every table backed by an IMap has a set of columns that are resolved automatically. Column resolution uses IMap entries
 * located on the member that initiates the query. The engine extracts columns from a key and a value and then merges them
 * into a single column set. In case the key and the value have columns with the same name, the key takes precedence.
 * <p>
 * Columns are extracted from objects as follows:
 * <ul>
 *     <li>For non-Portable objects, public getters and fields are used to populate the column list. For getters, the first
 *     letter is converted to lower case. A getter takes precedence over a field in case of naming conflict
 *     <li>For {@link Portable} objects, field names used in the {@link Portable#writePortable(PortableWriter)} method
 *     are used to populate the column list
 * </ul>
 * The whole key and value objects could be accessed through a special fields {@code __key} and {@code this}, respectively. If
 * key (value) object has fields, then the whole key (value) field is exposed as a normal field. Otherwise the field is hidden.
 * Hidden fields can be accessed directly, but are not returned by {@code SELECT * FROM ...} queries.
 * <p>
 * If the member that initiates a query doesn't have local entries for the given IMap, the query fails.
 * <p>
 * Consider the following key/value model:
 * <pre>
 *     class PersonKey {
 *         private long personId;
 *         private long deptId;
 *
 *         public long getPersonId() { ... }
 *         public long getDepartmentId() { ... }
 *     }
 *
 *     class Person {
 *         public String name;
 *     }
 * </pre>
 * This model will be resolved to the following table columns:
 * <ul>
 *     <li>personId BIGINT
 *     <li>departmentId BIGINT
 *     <li>name VARCHAR
 *     <li>__key OBJECT (hidden)
 *     <li>this OBJECT (hidden)
 * </ul>
 * <h2>Consistency</h2>
 * Results returned from IMap query are weakly consistent:
 * <ul>
 *     <li>If an entry was not updated during iteration, it is guaranteed to be returned exactly once
 *     <li>If an entry was modified during iteration, it might be returned zero, one or several times
 * </ul>
 * <h1>Usage</h1>
 * When a query is executed, an {@link SqlResult} is returned. You may get row iterator from the result. The result must be closed
 * at the end. The code snippet below demonstrates a typical usage pattern:
 * <pre>
 *     HazelcastInstance instance = ...;
 *
 *     try (SqlResult result = instance.sql().query("SELECT * FROM person")) {
 *         for (SqlRow row : result) {
 *             long personId = row.getObject("personId");
 *             String name = row.getObject("name");
 *             ...
 *         }
 *     }
 * </pre>
 */
@Beta
public interface SqlService {
    /**
     * Convenient method to execute a distributed query with the given parameters.
     * <p>
     * Converts passed SQL string and parameters into an {@link SqlQuery} object and invokes {@link #query(SqlQuery)}.
     *
     * @param sql SQL string
     * @param params query parameters that will be passed to {@link SqlQuery#setParameters(List)}
     * @return result
     * @throws NullPointerException if the SQL string is null
     * @throws IllegalArgumentException if the SQL string is empty
     * @throws SqlException in case of execution error
     *
     * @see SqlService
     * @see SqlQuery
     * @see #query(SqlQuery)
     */
    @Nonnull
    default SqlResult query(@Nonnull String sql, Object... params) {
        SqlQuery query = new SqlQuery(sql);

        if (params != null) {
            for (Object param : params) {
                query.addParameter(param);
            }
        }

        return query(query);
    }

    /**
     * Executes a distributed query.
     *
     * @param query query to be executed
     * @return result
     * @throws NullPointerException if the query is null
     * @throws SqlException in case of execution error
     *
     * @see SqlService
     */
    @Nonnull
    SqlResult query(@Nonnull SqlQuery query);
}

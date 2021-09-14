/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
 * The text below summarizes features supported by the SQL engine.
 * <p>
 * <h1>Overview</h1>
 * Hazelcast is able to execute distributed SQL queries over the following entities:
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
 * <h1>Querying an IMap</h1>
 * Before you can access an IMap, a <em>mapping</em> has to be created first. See the reference manual for the
 * CREATE MAPPING command.
 * <p>
 * <h2>Serialization Options</h2>
 * The <em>keyFormat</em> and <em>valueFormat</em> options are mandatory. Possible values for <em>keyFormat</em>
 * and <em>valueFormat</em>:
 * <ul>
 *     <li>java
 *     <li>portable
 *     <li>json
 * </ul>
 * <p>
 * <h3>Java Serialization</h3>
 * Java serialization uses the Java object exactly as <em>IMap.get()</em> returns it. You can use it for objects
 * serialized using the Java serialization or Hazelcast custom serialization ({@code DataSerializable} or
 * {@code IdentifiedDataSerializable}). For this format you must specify the class name using <em>keyJavaClass</em> and
 * <em>valueJavaClass</em> options. If you omit a column list from the <em>CREATE MAPPING</em> command, public getters
 * and fields are used to populate the column list - the first letter is converted to lower case and a getter takes
 * precedence over a field in case of naming conflict.
 * <p>
 * <h3>Portable Serialization</h3>
 * For this format, you need to specify additional options:
 * <ul>
 *     <li><em>keyPortableFactoryId</em>, <em>valuePortableFactoryId</em>
 *     <li><em>keyPortableClassId</em>, <em>valuePortableClassId</em>
 *     <li><em>keyPortableVersion</em>, <em>valuePortableVersion</em> : optional, default is <em>0</em>
 * </ul>
 * If you omit a column list from the <em>CREATE MAPPING</em> command, fields from {@code ClassDefinition} are used to
 * populate column list. If the {@code ClassDefinition} with the given IDs is not known to the cluster it will be
 * created based on the column list.
 * <p>
 * <h3>JSON Serialization</h3>
 * The value will be stored as {@code HazelcastJsonValue}. Hazelcast can't automatically determine the column list for
 * this format, you must explicitly specify it.
 * <p>
 * <h2>Hidden Fields</h2>
 * The whole key and value objects could be accessed through a special fields {@code __key} and {@code this},
 * respectively. They can be accessed directly, but are not returned by {@code SELECT * FROM ...} queries.
 * <p>
 * <h2>Consistency</h2>
 * Results returned from IMap query are weakly consistent:
 * <ul>
 *     <li>If an entry was not updated during iteration, it is guaranteed to be returned exactly once
 *     <li>If an entry was modified during iteration, it might be returned zero, one or several times
 * </ul>
 * <p>
 * <h1>Querying Kafka</h1>
 * Before you can access Kafka, a <em>mapping</em> has to be created first. See the reference manual for the
 * CREATE MAPPING command.
 * <p>
 * <h2>Serialization Options</h2>
 * The <em>keyFormat</em> and <em>valueFormat</em> options are mandatory. Possible values for <em>keyFormat</em>
 * and <em>valueFormat</em>:
 * <ul>
 *     <li>java
 *     <li>json
 *     <li>avro
 * </ul>
 * <p>
 * <h2>Hidden Fields</h2>
 * The whole key and value objects could be accessed through a special fields {@code __key} and {@code this},
 * respectively. They can be accessed directly, but are not returned by {@code SELECT * FROM ...} queries.
 * <p>
 * <h1>Querying Files</h1>
 * Before you can access Files, a <em>mapping</em> has to be created first. See the reference manual for the
 * CREATE MAPPING command.
 * <p>
 * <h2>Serialization Options</h2>
 * The <em>format</em> option is mandatory. Possible values for <em>format</em>:
 * <ul>
 *     <li>csv
 *     <li>json
 *     <li>avro
 *     <li>parquet
 * </ul>
 * <h1>Usage</h1>
 * When a query is executed, an {@link SqlResult} is returned. You may get row iterator from the result. The result must be closed
 * at the end. The code snippet below demonstrates a typical usage pattern:
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

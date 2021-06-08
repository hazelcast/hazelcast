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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.EventTimePolicySupplier;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.sql.SqlNodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An API to bridge Jet connectors and SQL. Allows the use of a Jet
 * connector from SQL and create a mapping for remote objects available
 * through the connector.
 * <p>
 * It's currently private because we expect significant changes to it, but
 * we plan to make it part of the public Core API in the future.
 */
public interface SqlConnector {

    /**
     * The serialization format. Used for connectors that don't have a separate
     * key and value format.
     */
    String OPTION_FORMAT = "format";

    /**
     * The key serialization format for key-value connectors.
     */
    String OPTION_KEY_FORMAT = "keyFormat";

    /**
     * The value serialization format for key-value connectors.
     */
    String OPTION_VALUE_FORMAT = "valueFormat";

    /**
     * The key Java class, if {@value #OPTION_KEY_FORMAT} is {@value
     * JAVA_FORMAT}.
     */
    String OPTION_KEY_CLASS = "keyJavaClass";

    /**
     * The value Java class, if {@value #OPTION_VALUE_FORMAT} is {@value
     * JAVA_FORMAT}.
     */
    String OPTION_VALUE_CLASS = "valueJavaClass";

    /**
     * The key Portable factory ID, if {@value #OPTION_KEY_FORMAT} is {@value
     * PORTABLE_FORMAT}.
     */
    String OPTION_KEY_FACTORY_ID = "keyPortableFactoryId";

    /**
     * The key Portable class ID, if {@value #OPTION_KEY_FORMAT} is {@value
     * PORTABLE_FORMAT}.
     */
    String OPTION_KEY_CLASS_ID = "keyPortableClassId";

    /**
     * The key Portable class version, if {@value #OPTION_KEY_FORMAT} is
     * {@value PORTABLE_FORMAT}.
     */
    String OPTION_KEY_CLASS_VERSION = "keyPortableClassVersion";

    /**
     * The value Portable factory ID, if {@value #OPTION_VALUE_FORMAT} is
     * {@value PORTABLE_FORMAT}.
     */
    String OPTION_VALUE_FACTORY_ID = "valuePortableFactoryId";

    /**
     * The value Portable class ID, if {@value #OPTION_VALUE_FORMAT} is {@value
     * PORTABLE_FORMAT}.
     */
    String OPTION_VALUE_CLASS_ID = "valuePortableClassId";

    /**
     * The value Portable class version, if {@value #OPTION_VALUE_FORMAT} is
     * {@value PORTABLE_FORMAT}.
     */
    String OPTION_VALUE_CLASS_VERSION = "valuePortableClassVersion";

    /**
     * Value for {@value #OPTION_KEY_FORMAT} and {@value #OPTION_VALUE_FORMAT}
     * for Java serialization.
     */
    String JAVA_FORMAT = "java";

    /**
     * Value for {@value #OPTION_KEY_FORMAT} and {@value #OPTION_VALUE_FORMAT}
     * for Portable serialization.
     */
    String PORTABLE_FORMAT = "portable";

    /**
     * Value for {@value #OPTION_KEY_FORMAT}, {@value #OPTION_VALUE_FORMAT}
     * and {@value #OPTION_FORMAT} for JSON serialization.
     */
    String JSON_FORMAT = "json";

    /**
     * Value for {@value #OPTION_FORMAT} for CSV (comma-separated values)
     * serialization.
     */
    String CSV_FORMAT = "csv";

    /**
     * Value for {@value #OPTION_FORMAT}, {@value #OPTION_KEY_FORMAT} and
     * {@value #OPTION_VALUE_FORMAT} for Avro serialization.
     */
    String AVRO_FORMAT = "avro";

    /**
     * Value for {@value #OPTION_FORMAT} for Parquet serialization.
     */
    String PARQUET_FORMAT = "parquet";

    /**
     * Return the name of the connector as seen in the {@code TYPE} clause in
     * the {@code CREATE EXTERNAL MAPPING} command.
     */
    String typeName();

    /**
     * Returns {@code true}, if {@link #fullScanReader} is an unbounded source.
     */
    boolean isStream();

    /**
     * Resolves the final field list given an initial field list and options
     * from the user. Jet calls this method when processing a CREATE MAPPING
     * statement.
     * <p>
     * The {@code userFields} can be empty, in this case the connector is
     * supposed to resolve them from a sample or from options. If it's not
     * empty, it should only validate it &mdash; it should neither add/remove
     * columns nor change the type. It can add the external name.
     * <p>
     * The returned list must not be empty. Jet stores it in the catalog, and
     * the user can see it by listing the catalog. Jet will later pass it to
     * {@link #createTable}.
     *
     * @param nodeEngine              an instance of {@link NodeEngine}
     * @param options                 user-provided options
     * @param userFields              user-provided list of fields, possibly empty
     * @param eventTimePolicySupplier supplier of {@link EventTimePolicy}
     * @return final field list, must not be empty
     */
    @Nonnull
    List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields,
            @Nullable EventTimePolicySupplier eventTimePolicySupplier
    );

    /**
     * Creates a {@link Table} object with the given fields. Should return
     * quickly; specifically it should not attempt to connect to the remote
     * service.
     * <p>
     * Jet calls this method for each statement execution and for each mapping.
     *
     * @param nodeEngine              an instance of {@link NodeEngine}
     * @param options                 connector specific options
     * @param resolvedFields          list of fields as returned from {@link
     *                                #resolveAndValidateFields}
     * @param eventTimePolicySupplier supplier of {@link EventTimePolicy}
     */
    @Nonnull
    Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields,
            @Nullable EventTimePolicySupplier eventTimePolicySupplier
    );

    /**
     * Returns a supplier for a source vertex reading the input according to
     * the {@code projection}/{@code predicate}. The output type of the source
     * is Object[].
     * <p>
     * The field indexes in the predicate and projection refer to the
     * zero-based indexes of the original fields of the {@code table}. For
     * example, if the table has fields {@code a, b, c} and the query is:
     * <pre>{@code
     *     SELECT b FROM t WHERE c=10
     * }</pre>
     * Then the projection will be {@code {1}} and the predicate will be {@code
     * {2}=10}.
     *
     * @param table      the table object
     * @param predicate  SQL expression to filter the rows
     * @param projection the list of field names to return
     * @return The DAG Vertex handling the reading
     */
    @Nonnull
    default Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        throw new UnsupportedOperationException("Full scan not supported for " + typeName());
    }

    /**
     * Creates a vertex to read the given {@code table} as a part of a
     * nested-loop join. The vertex will receive items from the left side of
     * the join as {@code Object[]}. For each record it must read the matching
     * records from the {@code table}, according to the {@code joinInfo} and
     * emit joined records, again as {@code Object[]}. The length of the output
     * array is {@code inputRecordLength + projection.size()}. See {@link
     * ExpressionUtil#join} for a utility to create output records.
     * <p>
     * The given {@code predicate} and {@code projection} apply only to the
     * records of the {@code table} (i.e. of the right-side of the join, before
     * joining). For example, if the table has fields {@code a, b, c} and the
     * query is:
     *
     * <pre>{@code
     *     SELECT l.v, r.b
     *     FROM l
     *     JOIN r ON l.v = r.b
     *     WHERE r.c=10
     * }</pre>
     * <p>
     * then the projection will be <code>{1}</code> and the predicate will be
     * <code>{2}=10</code>.
     * <p>
     * The predicates in the {@code joinInfo} apply to the joined record.
     * <p>
     * The implementation should do these steps for each received row:
     * <ul>
     *     <li>find rows in {@code table} matching the {@code predicate}
     *     <li>join all these rows using {@link ExpressionUtil#join} with the
     *         received row
     *     <li>if no rows matched and {@code joinInfo.isLeftOuter() == true)},
     *         the input row from the left side should be padded with {@code
     *         null}s and returned
     * </ul>
     *
     * @param table      the table object
     * @param predicate  SQL expression to filter the rows
     * @param projection the list of fields to return
     * @param joinInfo   {@link JetJoinInfo}
     * @return {@link VertexWithInputConfig}
     */
    @Nonnull
    default VertexWithInputConfig nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection,
            @Nonnull JetJoinInfo joinInfo
    ) {
        throw new UnsupportedOperationException("Nested-loop join not supported for " + typeName());
    }

    /**
     * Returns {@code true}, if {@code SINK INTO} is required instead of {@code
     * INSERT INTO} statements.
     * <p>
     * Unused for connectors that don't override {@link #sink}.
     */
    default boolean requiresSink() {
        return false;
    }

    /**
     * Returns the supplier for the sink processor.
     */
    @Nonnull
    default Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table table
    ) {
        throw new UnsupportedOperationException("INSERT INTO or SINK INTO not supported for " + typeName());
    }

    /**
     * Returns the supplier for the delete processor that will delete from the
     * given {@code table}. The input to the processor will be the fields
     * returned by {@link #getPrimaryKey(Table)}.
     */
    @Nonnull
    default Vertex deleteProcessor(@Nonnull DAG dag, @Nonnull Table table) {
        throw new UnsupportedOperationException("DELETE not supported for " + typeName());
    }

    /**
     * Return the indexes of fields that are primary key. These fields will be
     * fed to the delete processor.
     * <p>
     * Every connector that supports a {@link #deleteProcessor} should have a
     * primary key on each table, otherwise deleting cannot work. If some table
     * doesn't have a primary key and an empty node list is returned from this
     * method, an error will be thrown.
     */
    @Nonnull
    default SqlNodeList getPrimaryKey(Table table) {
        throw new UnsupportedOperationException("DELETE not supported by connector: " + typeName());
    }

    /**
     * Definition of a vertex along with a function to configure the input
     * edge(s).
     */
    class VertexWithInputConfig {

        private final Vertex vertex;
        private final Consumer<Edge> configureEdgeFn;

        /**
         * Creates a Vertex with default edge config (local, unicast).
         */
        public VertexWithInputConfig(Vertex vertex) {
            this(vertex, null);
        }

        public VertexWithInputConfig(Vertex vertex, Consumer<Edge> configureEdgeFn) {
            this.vertex = vertex;
            this.configureEdgeFn = configureEdgeFn;
        }

        public Vertex vertex() {
            return vertex;
        }

        public Consumer<Edge> configureEdgeFn() {
            return configureEdgeFn;
        }
    }
}

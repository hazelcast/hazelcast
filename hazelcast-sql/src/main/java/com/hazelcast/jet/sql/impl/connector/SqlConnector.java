/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

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
     * The key Compact type name, if {@value #OPTION_KEY_FORMAT} is {@value
     * COMPACT_FORMAT}.
     */
    String OPTION_KEY_COMPACT_TYPE_NAME = "keyCompactTypeName";

    /**
     * The value Compact type name, if {@value #OPTION_KEY_FORMAT} is {@value
     * COMPACT_FORMAT}.
     */
    String OPTION_VALUE_COMPACT_TYPE_NAME = "valueCompactTypeName";

    /**
     * Key record name in generated Avro schema if {@value #OPTION_KEY_FORMAT}
     * is {@value AVRO_FORMAT}. If not specified, defaults to {@code "jet.sql"}.
     */
    String OPTION_KEY_AVRO_RECORD_NAME = "keyAvroRecordName";

    /**
     * Value record name in generated Avro schema if {@value #OPTION_VALUE_FORMAT}
     * is {@value AVRO_FORMAT}. If not specified, defaults to {@code "jet.sql"}.
     */
    String OPTION_VALUE_AVRO_RECORD_NAME = "valueAvroRecordName";

    /**
     * Inline Avro schema for key if {@value #OPTION_KEY_FORMAT} is
     * {@value AVRO_FORMAT}.
     */
    String OPTION_KEY_AVRO_SCHEMA = AbstractHazelcastAvroSerde.OPTION_KEY_AVRO_SCHEMA;

    /**
     * Inline Avro schema for value if {@value #OPTION_VALUE_FORMAT} is
     * {@value AVRO_FORMAT}.
     */
    String OPTION_VALUE_AVRO_SCHEMA = AbstractHazelcastAvroSerde.OPTION_VALUE_AVRO_SCHEMA;

    /**
     * The class name of the Custom Type's underlying Java Class
     */
    String OPTION_TYPE_JAVA_CLASS = "javaClass";

    /**
     * The name of the Compact type used for Type
     */
    String OPTION_TYPE_COMPACT_TYPE_NAME = "compactTypeName";

    String OPTION_TYPE_PORTABLE_FACTORY_ID = "portableFactoryId";

    String OPTION_TYPE_PORTABLE_CLASS_ID = "portableClassId";

    String OPTION_TYPE_PORTABLE_CLASS_VERSION = "portableClassVersion";

    String OPTION_TYPE_AVRO_SCHEMA = "avroSchema";

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
     * Value for {@value #OPTION_KEY_FORMAT} and {@value #OPTION_VALUE_FORMAT}
     * for Compact serialization.
     */
    String COMPACT_FORMAT = "compact";

    /**
     * Value for {@value #OPTION_KEY_FORMAT}, {@value #OPTION_VALUE_FORMAT}
     * and {@value #OPTION_FORMAT} for JSON serialization.
     */
    String JSON_FLAT_FORMAT = "json-flat";

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
     * Returns default value for the Object Type mapping property, so if user won't provide any type,
     * this will be assumed.
     *
     * @since 5.3
     */
    @Nonnull
    String defaultObjectType();

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
     * @param nodeEngine       an instance of {@link NodeEngine}
     * @param externalResource an object for which fields should be resolved
     * @param userFields       user-provided list of fields, possibly empty
     * @return final field list, must not be empty
     */
    @Nonnull
    List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields
    );

    /**
     * Returns the required permissions to execute
     * {@link #resolveAndValidateFields(NodeEngine, SqlExternalResource, List)} method.
     * <p>
     * Implementors of {@link SqlConnector} don't need to override this method when {@code resolveAndValidateFields}
     * doesn't support field resolution or when validation doesn't access the external resource.
     * <p>
     * The permissions are usually the same as required permissions to read from the external resource.
     *
     * @return list of permissions required to run {@link #resolveAndValidateFields}
     */
    @Nonnull
    default List<Permission> permissionsForResolve(SqlExternalResource resource, NodeEngine nodeEngine) {
        return emptyList();
    }

    /**
     * Creates a {@link Table} object with the given fields. Should return
     * quickly; specifically it should not attempt to connect to the remote
     * service.
     * <p>
     * Jet calls this method for each statement execution and for each mapping.
     *
     * @param nodeEngine       an instance of {@link NodeEngine}
     * @param externalResource an object for which this table is created
     * @param resolvedFields   list of fields as returned from {@link
     *                         #resolveAndValidateFields}
     */
    @Nonnull
    Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields);

    /**
     * Returns a supplier for a source vertex reading the input according to
     * the {@code projection}/{@code predicate}. The output type of the source
     * is {@link JetSqlRow}.
     * <p>
     * The field indexes in the predicate and projection refer to the
     * zero-based indexes of the original fields of the {@code table}. For
     * example, if the table has fields {@code a, b, c} and the query is:
     * <pre>{@code
     *     SELECT b FROM t WHERE c=10
     * }</pre>
     * Then the projection will be {@code {1}} and the predicate will be {@code
     * {2}=10}.
     * <p>
     * If the implementation cannot generate watermarks, it should throw, if the
     * {@code eventTimePolicyProvider} is not null. Streaming sources should
     * support it, batch sources don't have to.
     *
     * @param predicate                SQL expression to filter the rows
     * @param projection               the list of field names to return
     * @param requiredPartitionsToScan the set of partitions to scan,
     *                                 if partitioning strategy is used
     * @param eventTimePolicyProvider  {@link EventTimePolicy}
     * @return The DAG Vertex handling the reading
     */
    @Nonnull
    default Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        throw new UnsupportedOperationException("Full scan not supported for " + typeName());
    }

    /**
     * Creates a vertex to read the given {@code table} as a part of a
     * nested-loop join. The vertex will receive items from the left side of
     * the join as {@link JetSqlRow}. For each record it must read the matching
     * records from the {@code table}, according to the {@code joinInfo} and
     * emit joined records, again as {@link JetSqlRow}. The number of fields in
     * the output row is {@code inputRecordLength + projection.size()}. See
     * {@link ExpressionUtil#join} for a utility to create output rows.
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
     * @param predicate  SQL expression to filter the rows
     * @param projection the list of fields to return
     * @param joinInfo   {@link JetJoinInfo}
     * @return {@link VertexWithInputConfig}
     */
    @Nonnull
    default VertexWithInputConfig nestedLoopReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nonnull JetJoinInfo joinInfo
    ) {
        throw new UnsupportedOperationException("Nested-loop join not supported for " + typeName());
    }

    default boolean isNestedLoopReaderSupported() {
        try {
            // nestedLoopReader() is supported, if the class overrides the default method in this class
            Method m = getClass().getMethod("nestedLoopReader", DagBuildContext.class, HazelcastRexNode.class,
                    List.class,
                    JetJoinInfo.class);
            return m.getDeclaringClass() != SqlConnector.class;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the supplier for the insert processor.
     */
    @Nonnull
    default VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        throw new UnsupportedOperationException("INSERT INTO not supported for " + typeName());
    }

    /**
     * Returns the supplier for the sink processor.
     */
    @Nonnull
    default Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        throw new UnsupportedOperationException("SINK INTO not supported for " + typeName());
    }

    /**
     * Returns the supplier for the update processor that will update the given
     * {@code table}.
     * <p>
     * The processor is expected to work in a different mode, depending on the
     * `hasInput` argument:<ol>
     *
     * <li><b>hasInput == false:</b> There will be no input to the
     * processor. The processor is supposed to update all rows matching the
     * given `predicate`. If the `predicate` is null, it's supposed to
     * update all rows. The `expressions` have no input references.
     *
     * <li><b>hasInput == true:</b> The processor is supposed to update all
     * rows with primary keys it receives on the input. In this mode the
     * `predicate` is always null. The primary key fields are specified by
     * the {@link #getPrimaryKey(Table)} method. If {@link
     * #dmlSupportsPredicates()} returned false, or if {@link
     * #supportsExpression} always returns false, `hasInput` is always true.
     * The `expressions` might contain input references. The input's first
     * columns are the primary key values, the rest are values that might be
     * referenced by expressions.
     *
     * </ol>
     *
     * @param fieldNames  The names of fields to update
     * @param expressions The expressions to assign to each field. Has the same
     *                    length as {@code fieldNames}.
     */
    @Nonnull
    default Vertex updateProcessor(
            @Nonnull DagBuildContext context,
            @Nonnull List<String> fieldNames,
            @Nonnull List<HazelcastRexNode> expressions,
            @Nullable HazelcastRexNode predicate,
            boolean hasInput
    ) {
        throw new UnsupportedOperationException("UPDATE not supported for " + typeName());
    }

    /**
     * Returns the supplier for the delete processor that will delete from the
     * given {@code table}.
     * <p>
     * The processor is expected to work in a different mode, depending on the
     * `hasInput` argument:<ol>
     *
     * <li><b>hasInput == false:</b> There will be no input to the
     * processor. The processor is supposed to update all rows matching the
     * given `predicate`. If the `predicate` is null, it's supposed to
     * update all rows.
     *
     * <li><b>hasInput == true:</b> The processor is supposed to delete all
     * rows with primary keys it receives on the input. In this mode the
     * `predicate` is always null. The primary key fields are specified by
     * the {@link #getPrimaryKey(Table)} method. If {@link
     * #dmlSupportsPredicates()} returned false, or if {@link
     * #supportsExpression} always returns false, `hasInput` is always true.
     *
     * </ol>
     */
    @Nonnull
    default Vertex deleteProcessor(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            boolean hasInput
    ) {
        throw new UnsupportedOperationException("DELETE not supported for " + typeName());
    }

    /**
     * Returns whether this processor's {@link #deleteProcessor} and {@link
     * #updateProcessor} methods can use a predicate.
     */
    default boolean dmlSupportsPredicates() {
        return true;
    }

    /**
     * Returns whether the given `expression` is supported by the processors
     * this connector returns. If it returns true, then this expression will be
     * passed as a projection or predicate to the other vertex-generating
     * methods.
     * <p>
     * The connector must be able to handle {@link RexInputRef} expressions,
     * such expressions will never be passed to this method. It is also
     * recommended to support {@link RexDynamicParam} and {@link RexLiteral} for
     * simpler execution plans.
     * <p>
     * If an expression is unsupported, for scans a projection will be added
     * after the scan vertex. For DML, it will disable the use no-input mode.
     * Instead, a scan and filtering will be generated.
     * <p>
     * The default implementation returns true for {@link RexDynamicParam}.
     *
     * @param expression expression to be analysed. Entire expression must be
     *                   checked, not only the root node.
     * @return true, iff the given expression can be evaluated remotely
     */
    default boolean supportsExpression(@Nonnull HazelcastRexNode expression) {
        // support only simple RexDynamicParam ref. If RexDynamicParam is inside larger expression,
        // entire expression will have to be analysed.
        return expression.unwrap(RexNode.class) instanceof RexDynamicParam;
    }

    /**
     * Return the indexes of fields that are primary key. These fields will be
     * fed to the delete and update processors.
     * <p>
     * Every connector that supports {@link #deleteProcessor} or
     * {@link #updateProcessor} should have a primary key on each table,
     * otherwise deleting/updating cannot work. If some table doesn't have a
     * primary key and an empty node list is returned from this method, an error
     * will be thrown.
     */
    @Nonnull
    default List<String> getPrimaryKey(Table table) {
        throw new UnsupportedOperationException("PRIMARY KEY not supported by connector: " + typeName());
    }

    /**
     * Return the set of options that are not sensitive
     * to be displayed by querying {@code information_schema}.
     */
    default Set<String> nonSensitiveConnectorOptions() {
        Set<String> options = new HashSet<>();
        options.add(OPTION_FORMAT);
        options.add(OPTION_KEY_FORMAT);
        options.add(OPTION_VALUE_FORMAT);
        options.add(OPTION_KEY_CLASS);
        options.add(OPTION_VALUE_CLASS);
        options.add(OPTION_KEY_FACTORY_ID);
        options.add(OPTION_KEY_CLASS_ID);
        options.add(OPTION_KEY_CLASS_VERSION);
        options.add(OPTION_VALUE_FACTORY_ID);
        options.add(OPTION_VALUE_CLASS_ID);
        options.add(OPTION_VALUE_CLASS_VERSION);
        options.add(OPTION_KEY_COMPACT_TYPE_NAME);
        options.add(OPTION_VALUE_COMPACT_TYPE_NAME);
        options.add(OPTION_KEY_AVRO_RECORD_NAME);
        options.add(OPTION_VALUE_AVRO_RECORD_NAME);
        options.add(OPTION_KEY_AVRO_SCHEMA);
        options.add(OPTION_VALUE_AVRO_SCHEMA);
        options.add(OPTION_TYPE_JAVA_CLASS);
        options.add(OPTION_TYPE_COMPACT_TYPE_NAME);
        options.add(OPTION_TYPE_PORTABLE_FACTORY_ID);
        options.add(OPTION_TYPE_PORTABLE_CLASS_ID);
        options.add(OPTION_TYPE_PORTABLE_CLASS_VERSION);
        options.add(OPTION_TYPE_AVRO_SCHEMA);
        return options;
    }

    interface DagBuildContext {

        @Nonnull
        NodeEngine getNodeEngine();

        /**
         * Returns the {@link DAG} that's being created.
         */
        @Nonnull
        DAG getDag();

        /**
         * Returns the context table. It is:<ul>
         * <li>for scans, it's the scanned table
         * <li>for DML, it's the target table
         * <li>for nested loop reader, it's the table read in the inner loop (the right join input)
         * </ul>
         *
         * @throws IllegalStateException if the table doesn't apply in the current context
         */
        @Nonnull
        <T extends Table> T getTable();

        /**
         * Converts a boolean {@link HazelcastRexNode}. When evaluating a {@link RexInputRef},
         * this context is assumed:<ul>
         * <li>If a table is set (see {@link #getTable()}), then it's assumed that it's that table's fields
         * <li>If it's a single-input rel, then it's then input
         * <li>Otherwise the conversion of an input ref will fail.
         * </ul>
         */
        @Nullable
        Expression<Boolean> convertFilter(@Nullable HazelcastRexNode node);

        /**
         * Converts a list of {@link HazelcastRexNode}. See also {@link #convertFilter(HazelcastRexNode)}
         * for information about {@link RexInputRef} conversion.
         */
        @Nonnull
        List<Expression<?>> convertProjection(@Nonnull List<HazelcastRexNode> nodes);
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

    /**
     * Data describing external resource (table, topic, stream, IMap etc)
     *
     * @since 5.3
     */
    class SqlExternalResource implements Serializable {
        private final String[] externalName;
        private final String dataConnection;
        private final String connectorType;
        private final String objectType;
        private final Map<String, String> options;

        public SqlExternalResource(@Nonnull String[] externalName, String dataConnection, @Nonnull String connectorType,
                                   String objectType, Map<String, String> options) {
            this.externalName = requireNonNull(externalName, "externalName cannot be null");
            this.dataConnection = dataConnection;
            this.connectorType = requireNonNull(connectorType, "connectorType cannot be null");
            this.objectType = objectType;
            this.options = options;
        }

        /**
         * Name of external object.
         */
        @Nonnull
        public String[] externalName() {
            return externalName;
        }

        /**
         * Name of the data connection to use, may be null if the connector supports specifying connection details in options
         */
        @Nullable
        public String dataConnection() {
            return dataConnection;
        }

        /**
         * Connector Type, must match one of {@link SqlConnector#typeName()}.
         */
        @Nonnull
        public String connectorType() {
            return connectorType;
        }

        /**
         * Object type, must match the name of supported types for given connector.
         */
        @Nullable
        public String objectType() {
            return objectType;
        }

        /**
         * Options used to create the mapping.
         */
        @Nonnull
        public Map<String, String> options() {
            return options == null ? emptyMap() : options;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlExternalResource that = (SqlExternalResource) o;
            return Arrays.equals(externalName, that.externalName)
                    && Objects.equals(dataConnection, that.dataConnection)
                    && Objects.equals(connectorType, that.connectorType)
                    && Objects.equals(objectType, that.objectType)
                    && Objects.equals(options, that.options);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(dataConnection, connectorType, objectType, options);
            result = 31 * result + Arrays.hashCode(externalName);
            return result;
        }
    }
}

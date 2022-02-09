/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.test;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.resolveTypeForTypeFamily;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;

/**
 * A test connector. It emits rows of provided types and values. It
 * emits a slice of the rows on each member.
 */
public abstract class TestAbstractSqlConnector implements SqlConnector {

    private static final String OPTION_NAMES = "names";
    private static final String OPTION_TYPES = "types";
    private static final String OPTION_VALUES = "values";
    private static final String OPTION_STREAMING = "streaming";

    private static final String DELIMITER = ",";
    private static final String VALUES_DELIMITER = "\n";
    private static final String NULL = "null";

    static void create(
            SqlService sqlService,
            String connectorType,
            String tableName,
            List<String> names,
            List<QueryDataTypeFamily> types,
            List<String[]> values,
            boolean streamingActual
    ) {
        if (names.stream().anyMatch(n -> n.contains(DELIMITER) || n.contains("'"))) {
            throw new IllegalArgumentException("'" + DELIMITER + "' and apostrophe not supported in names");
        }

        if (types.contains(QueryDataTypeFamily.OBJECT) || types.contains(QueryDataTypeFamily.NULL)) {
            throw new IllegalArgumentException("NULL and OBJECT type not supported: " + types);
        }

        if (values.stream().flatMap(Arrays::stream).filter(Objects::nonNull)
                .anyMatch(n -> n.equals(NULL) || n.contains(VALUES_DELIMITER) || n.contains("'"))
        ) {
            throw new IllegalArgumentException("The text '" + NULL + "', the newline character and apostrophe not " +
                    "supported in values");
        }

        String namesSerialized = join(DELIMITER, names);
        String typesSerialized = types.stream().map(QueryDataTypeFamily::name).collect(joining(DELIMITER));
        String valuesSerialized = values.stream().map(row -> join(DELIMITER, row)).collect(joining(VALUES_DELIMITER));

        String sql = "CREATE MAPPING " + tableName + " TYPE " + connectorType
                + " OPTIONS ("
                + '\'' + OPTION_NAMES + "'='" + namesSerialized + "'"
                + ", '" + OPTION_TYPES + "'='" + typesSerialized + "'"
                + ", '" + OPTION_VALUES + "'='" + valuesSerialized + "'"
                + ", '" + OPTION_STREAMING + "'='" + streamingActual + "'"
                + ")";
        System.out.println(sql);
        sqlService.execute(sql).updateCount();
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        if (userFields.size() > 0) {
            throw QueryException.error("Don't specify external fields, they are fixed");
        }

        String[] names = options.get(OPTION_NAMES).split(DELIMITER);
        String[] types = options.get(OPTION_TYPES).split(DELIMITER);

        assert names.length == types.length;

        List<MappingField> fields = new ArrayList<>(names.length);
        for (int i = 0; i < names.length; i++) {
            fields.add(new MappingField(names[i], resolveTypeForTypeFamily(QueryDataTypeFamily.valueOf(types[i]))));
        }
        return fields;
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        String[] names = options.get(OPTION_NAMES).split(DELIMITER);
        String[] types = options.get(OPTION_TYPES).split(DELIMITER);

        assert names.length == types.length;

        List<TableField> fields = new ArrayList<>(names.length);
        for (int i = 0; i < names.length; i++) {
            fields.add(new TableField(names[i], resolveTypeForTypeFamily(QueryDataTypeFamily.valueOf(types[i])), false));
        }

        List<Object[]> rows = new ArrayList<>();
        String[] rowsSerialized = options.get(OPTION_VALUES).split(VALUES_DELIMITER);
        for (String rowSerialized : rowsSerialized) {
            if (rowSerialized.isEmpty()) {
                continue;
            }

            String[] values = rowSerialized.split(DELIMITER);

            assert values.length == fields.size();

            Object[] row = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                if (NULL.equals(value)) {
                    row[i] = null;
                } else {
                    row[i] = fields.get(i).getType().convert(values[i]);
                }
            }
            rows.add(row);
        }

        boolean streaming = Boolean.parseBoolean(options.get(OPTION_STREAMING));
        return new TestTable(this, schemaName, mappingName, fields, rows, streaming);
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table_,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        TestTable table = (TestTable) table_;
        List<Object[]> rows = table.rows;
        boolean streaming = table.streaming;

        FunctionEx<Context, TestDataGenerator> createContextFn = ctx -> {
            ExpressionEvalContext evalContext = ExpressionEvalContext.from(ctx);
            EventTimePolicy<JetSqlRow> eventTimePolicy = eventTimePolicyProvider == null
                    ? EventTimePolicy.noEventTime()
                    : eventTimePolicyProvider.apply(evalContext);
            return new TestDataGenerator(rows, predicate, projection, evalContext, eventTimePolicy, streaming);
        };

        ProcessorMetaSupplier pms = createProcessorSupplier(createContextFn);
        return dag.newUniqueVertex(table.toString(), pms);
    }

    protected abstract ProcessorMetaSupplier createProcessorSupplier(FunctionEx<Context, TestDataGenerator> createContextFn);

    private static final class TestTable extends JetTable {

        private final List<Object[]> rows;
        private final boolean streaming;

        private TestTable(
                @Nonnull SqlConnector sqlConnector,
                @Nonnull String schemaName,
                @Nonnull String name,
                @Nonnull List<TableField> fields,
                @Nonnull List<Object[]> rows,
                boolean streaming
        ) {
            super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(rows.size()));
            this.rows = rows;
            this.streaming = streaming;
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return new TestTablePlanObjectKey(getSchemaName(), getSqlName(), rows);
        }
    }

    private static final class TestTablePlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String name;
        private final List<Object[]> rows;

        private TestTablePlanObjectKey(String schemaName, String name, List<Object[]> rows) {
            this.schemaName = schemaName;
            this.name = name;
            this.rows = rows;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestTablePlanObjectKey that = (TestTablePlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName)
                    && Objects.equals(name, that.name)
                    && Objects.equals(rows, that.rows);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, name, rows);
        }
    }

    static final class TestDataGenerator {

        private static final int MAX_BATCH_SIZE = 1024;

        private final Traverser<Object> traverser;
        //private final Iterator<Object[]> iterator;
        private final boolean streaming;

        private TestDataGenerator(
                List<Object[]> rows,
                Expression<Boolean> predicate,
                List<Expression<?>> projections,
                ExpressionEvalContext evalContext,
                EventTimePolicy<JetSqlRow> eventTimePolicy,
                boolean streaming
        ) {
            EventTimeMapper<JetSqlRow> eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
            eventTimeMapper.addPartitions(1);
            this.traverser = Traversers.traverseIterable(rows)
                    .flatMap(row -> {
                        JetSqlRow evaluated = ExpressionUtil.evaluate(predicate, projections, new JetSqlRow(evalContext.getSerializationService(), row), evalContext);
                        return evaluated == null ? Traversers.empty() : eventTimeMapper.flatMapEvent(evaluated, 0, -1);
                    });

            this.streaming = streaming;
        }

        void fillBuffer(SourceBuilder.SourceBuffer<Object> buffer) {
            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                Object o;
                if ((o = traverser.next()) != null) {
                    buffer.add(o);
                } else {
                    if (!streaming) {
                        buffer.close();
                    }
                    return;
                }
            }
        }
    }
}

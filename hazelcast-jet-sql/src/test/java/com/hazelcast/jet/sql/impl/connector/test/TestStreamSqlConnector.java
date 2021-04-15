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

package com.hazelcast.jet.sql.impl.connector.test;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.NOT_IMPLEMENTED_ARGUMENTS_CONTEXT;
import static java.util.Collections.singletonList;

/**
 * A connector for SQL that has a single column `v` of type BIGINT and
 * emits 100 items per second using {@link TestSources#itemStream(int)}.
 */
public class TestStreamSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "TestStream";

    private static final List<MappingField> FIELD_LIST = singletonList(new MappingField("v", QueryDataType.BIGINT));

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Nonnull @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        if (userFields.size() > 0) {
            throw QueryException.error("Don't specify external fields, they are fixed");
        }
        return FIELD_LIST;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        return new TestStreamTable(
                this,
                schemaName,
                mappingName,
                toList(resolvedFields, field -> new TableField(field.name(), field.type(), false))
        );
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nonnull @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        StreamSourceTransform<Object[]> source = (StreamSourceTransform<Object[]>) TestSources.itemStream(100,
                (timestamp, sequence) ->
                        ExpressionUtil.evaluate(predicate, projection, new Object[]{sequence},
                                NOT_IMPLEMENTED_ARGUMENTS_CONTEXT));
        ProcessorMetaSupplier pms = source.metaSupplierFn.apply(EventTimePolicy.noEventTime());
        return dag.newUniqueVertex(table.toString(), pms);
    }

    private static final class TestStreamTable extends JetTable {

        private TestStreamTable(
                @Nonnull SqlConnector sqlConnector,
                @Nonnull String schemaName,
                @Nonnull String name,
                @Nonnull List<TableField> fields
        ) {
            super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(Long.MAX_VALUE));
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return new TestStreamPlanObjectKey(getSchemaName(), getSqlName());
        }
    }

    private static final class TestStreamPlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String name;

        private TestStreamPlanObjectKey(String schemaName, String name) {
            this.schemaName = schemaName;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestStreamPlanObjectKey that = (TestStreamPlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, name);
        }
    }
}

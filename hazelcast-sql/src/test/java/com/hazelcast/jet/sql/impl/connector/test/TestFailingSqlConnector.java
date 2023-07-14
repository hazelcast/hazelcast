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

package com.hazelcast.jet.sql.impl.connector.test;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Collections.singletonList;

/**
 * A SQL source that fails immediately.
 */
public class TestFailingSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "FailingSource";

    private static final List<MappingField> FIELD_LIST = singletonList(new MappingField("v", QueryDataType.BIGINT));

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "Dummy";
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        if (userFields.size() > 0) {
            throw QueryException.error("Don't specify external fields, they are fixed");
        }
        return FIELD_LIST;
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        return new TestFailingTable(
                this,
                schemaName,
                mappingName,
                toList(resolvedFields, field -> new TableField(field.name(), field.type(), false))
        );
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }

        return context.getDag().newUniqueVertex(
                "FailingSource[" + context.getTable().getSchemaName() + "." + context.getTable().getSqlName() + ']',
                FailingP::new
        );
    }

    private static final class FailingP extends AbstractProcessor {
        @Override
        public boolean complete() {
            throw new RuntimeException("mock failure");
        }
    }

    private static final class TestFailingTable extends JetTable {

        private TestFailingTable(
                @Nonnull SqlConnector sqlConnector,
                @Nonnull String schemaName,
                @Nonnull String name,
                @Nonnull List<TableField> fields
        ) {
            super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(0), null, true);
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return new TestFailingPlanObjectKey(getSchemaName(), getSqlName());
        }
    }

    private static final class TestFailingPlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String name;

        private TestFailingPlanObjectKey(String schemaName, String name) {
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
            TestFailingPlanObjectKey that = (TestFailingPlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, name);
        }
    }
}

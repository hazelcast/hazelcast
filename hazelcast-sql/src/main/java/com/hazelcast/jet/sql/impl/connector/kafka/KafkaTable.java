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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

class KafkaTable extends JetTable {

    private final QueryTargetDescriptor keyQueryDescriptor;
    private final UpsertTargetDescriptor keyUpsertDescriptor;

    private final QueryTargetDescriptor valueQueryDescriptor;
    private final UpsertTargetDescriptor valueUpsertDescriptor;

    private final String topicName;
    private final Map<String, String> options;

    @SuppressWarnings("checkstyle:ParameterNumber")
    KafkaTable(
            SqlConnector sqlConnector,
            String schemaName,
            String name,
            List<TableField> fields,
            TableStatistics statistics,
            String topicName,
            Map<String, String> options,
            QueryTargetDescriptor keyQueryDescriptor,
            UpsertTargetDescriptor keyUpsertDescriptor,
            QueryTargetDescriptor valueQueryDescriptor,
            UpsertTargetDescriptor valueUpsertDescriptor
    ) {
        super(sqlConnector, fields, schemaName, name, statistics);

        this.keyQueryDescriptor = keyQueryDescriptor;
        this.keyUpsertDescriptor = keyUpsertDescriptor;

        this.valueQueryDescriptor = valueQueryDescriptor;
        this.valueUpsertDescriptor = valueUpsertDescriptor;

        this.topicName = topicName;
        this.options = options;
    }

    String topicName() {
        return topicName;
    }

    Properties kafkaConsumerProperties() {
        return PropertiesResolver.resolveConsumerProperties(options);
    }

    Properties kafkaProducerProperties() {
        return PropertiesResolver.resolveProducerProperties(options);
    }

    QueryTargetDescriptor keyQueryDescriptor() {
        return keyQueryDescriptor;
    }

    UpsertTargetDescriptor keyUpsertDescriptor() {
        return keyUpsertDescriptor;
    }

    QueryTargetDescriptor valueQueryDescriptor() {
        return valueQueryDescriptor;
    }

    UpsertTargetDescriptor valueUpsertDescriptor() {
        return valueUpsertDescriptor;
    }

    QueryPath[] paths() {
        return getFields().stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
    }

    QueryDataType[] types() {
        return getFields().stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return new KafkaPlanObjectKey(getSchemaName(), getSqlName(), topicName(), getFields(), options);
    }

    static final class KafkaPlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String tableName;
        private final String topicName;
        private final List<TableField> fields;
        private final Map<String, String> options;

        KafkaPlanObjectKey(
                String schemaName,
                String tableName,
                String topicName,
                List<TableField> fields,
                Map<String, String> options
        ) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.topicName = topicName;
            this.fields = fields;
            this.options = options;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KafkaPlanObjectKey that = (KafkaPlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(topicName, that.topicName)
                    && Objects.equals(fields, that.fields)
                    && Objects.equals(options, that.options);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, tableName, topicName, fields, options);
        }
    }
}

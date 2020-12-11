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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Map;
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
}

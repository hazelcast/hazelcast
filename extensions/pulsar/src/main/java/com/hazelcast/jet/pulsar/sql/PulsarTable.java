/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar.sql;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.jet.pulsar.PulsarSchema;
import com.hazelcast.jet.pulsar.impl.PulsarConfig;
import com.hazelcast.jet.pulsar.impl.PulsarSinkConfig;
import com.hazelcast.jet.pulsar.impl.PulsarSourceConfig;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.JavaClassNameResolver;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.TopicName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_PREFERRED_LOCAL_PARALLELISM;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

class PulsarTable extends JetTable {
    static final String PULSAR_OPTION_PREFIX = "pulsar.";
    static final String OPTION_BROKER_URL = PulsarDataConnection.Properties.BROKER_URL;
    static final String OPTION_ENABLE_TRANSACTIONS = PulsarDataConnection.Properties.ENABLE_TRANSACTIONS;
    static final String OPTION_HTTP_SERVICE_URL = PulsarDataConnection.Properties.HTTP_SERVICE_URL;
    private final String topicName;
    private final String dataConnectionName;
    private final Map<String, String> options;
    private transient Map<String, Object> pulsarOptions;

    private final QueryTargetDescriptor keyQueryDescriptor;
    private final UpsertTargetDescriptor keyUpsertDescriptor;

    private final QueryTargetDescriptor valueQueryDescriptor;
    private final UpsertTargetDescriptor valueUpsertDescriptor;

    @SuppressWarnings("checkstyle:ParameterNumber")
    PulsarTable(
            SqlConnector sqlConnector,
            String schemaName,
            String name,
            List<TableField> fields,
            TableStatistics statistics,
            String topicName,
            String dataConnectionName,
            Map<String, String> options,
            QueryTargetDescriptor keyQueryDescriptor,
            UpsertTargetDescriptor keyUpsertDescriptor,
            QueryTargetDescriptor valueQueryDescriptor,
            UpsertTargetDescriptor valueUpsertDescriptor,
            String objectType) {
        super(sqlConnector, fields, schemaName, name, statistics, objectType, true);
        this.topicName = topicName;
        this.dataConnectionName = dataConnectionName;
        this.options = options;

        this.keyQueryDescriptor = keyQueryDescriptor;
        this.keyUpsertDescriptor = keyUpsertDescriptor;

        this.valueQueryDescriptor = valueQueryDescriptor;
        this.valueUpsertDescriptor = valueUpsertDescriptor;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    <T extends PulsarConfig> T buildConfigForSql(Supplier<? extends T> builder) {
        var conf = builder.get();
        if (conf instanceof PulsarSourceConfig sc) {
            sc.setTopics(List.of(topicName()));
        } else if (conf instanceof PulsarSinkConfig sc) {
            sc.setTopic(topicName());
        }
        conf.setSchemaSupplier(resolveSchema());
        conf.setOptions(pulsarOptions());

        if (dataConnectionName() != null) {
            conf.setDataConnectionRef(DataConnectionRef.dataConnectionRef(dataConnectionName()));
        } else {
            final String brokerUrl = options.get(OPTION_BROKER_URL);
            final boolean enableTransactions = Boolean.parseBoolean(options.getOrDefault(OPTION_ENABLE_TRANSACTIONS, "false"));
            if (brokerUrl != null) {
                conf.setConnectionSupplier(
                    () -> PulsarClient.builder().serviceUrl(brokerUrl).enableTransaction(enableTransactions).build());
            } else {
                final String brokerUrlPulsarDot = options.get(PULSAR_OPTION_PREFIX + OPTION_BROKER_URL);
                final boolean enableTransactionsPulsarDot =
                    Boolean.parseBoolean(options.getOrDefault(PULSAR_OPTION_PREFIX + OPTION_ENABLE_TRANSACTIONS, "false"));
                if (brokerUrlPulsarDot != null) {
                    conf.setConnectionSupplier(() -> PulsarClient.builder().serviceUrl(brokerUrlPulsarDot).enableTransaction(
                        enableTransactionsPulsarDot).build());
                }
            }

            String httpServiceUrl = options.get(OPTION_HTTP_SERVICE_URL);
            if (httpServiceUrl != null) {
                conf.setHttpServiceUrl(httpServiceUrl);
            } else {
                String pulsarDotHttpServiceUrl = options.get(PULSAR_OPTION_PREFIX + OPTION_HTTP_SERVICE_URL);
                if (pulsarDotHttpServiceUrl != null) {
                    conf.setHttpServiceUrl(pulsarDotHttpServiceUrl);
                }
            }
        }
        return conf;
    }

    /**
     * SQL connector options starting with {@code pulsar.} will be translated into Pulsar options for source/sink.
     */
    Map<String, Object> pulsarOptions() {
        if (pulsarOptions == null) {
            pulsarOptions = new HashMap<>();
            for (var entry : options.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(PULSAR_OPTION_PREFIX)) {
                    String pulsarSpecificKey = key.substring(PULSAR_OPTION_PREFIX.length());
                    pulsarOptions.put(pulsarSpecificKey, entry.getValue());
                }
            }
        }
        return pulsarOptions;
    }

    @SuppressWarnings("rawtypes")
    public SupplierEx resolveSchema() {
        String format = options.get(OPTION_VALUE_FORMAT);

        if (JSON_FLAT_FORMAT.equals(format)) {
            return PulsarSchema.bytes().supplierV4();
        } else if ("json".equals(format)) {
            return PulsarJsonSchema::new;
        } else if (JAVA_FORMAT.equals(format)) {
            String clazz = options.get(OPTION_VALUE_CLASS);
            SupplierEx resolvedSerializer = resolveSerializer(clazz);
            if (resolvedSerializer != null) {
                return resolvedSerializer;
            }
        } else if (AVRO_FORMAT.equals(format)) {
            return PulsarAvroSchema.supplier(valueUpsertDescriptor.getSchema().toString());
        } else {
            String resolvedClass = JavaClassNameResolver.resolveClassName(format);
            if (resolvedClass != null) {
                SupplierEx resolvedSerializer = resolveSerializer(resolvedClass);
                if (resolvedSerializer != null) {
                    return resolvedSerializer;
                }
            }
        }
        throw new UnsupportedOperationException("Unsupported format: " + format);
    }

    @SuppressWarnings({ "rawtypes", "checkstyle:ReturnCount" })
    private static SupplierEx resolveSerializer(String clazz) {
        if (Byte.class.getName().equals(clazz) || byte.class.getName().equals(clazz)) {
            return PulsarSchema.int8().supplierV4();
        } else if (Short.class.getName().equals(clazz) || short.class.getName().equals(clazz)) {
            return PulsarSchema.int16().supplierV4();
        } else if (Integer.class.getName().equals(clazz) || int.class.getName().equals(clazz)) {
            return PulsarSchema.int32().supplierV4();
        } else if (Long.class.getName().equals(clazz) || long.class.getName().equals(clazz)) {
            return PulsarSchema.int64().supplierV4();
        } else if (Float.class.getName().equals(clazz) || float.class.getName().equals(clazz)) {
            return PulsarSchema.float32().supplierV4();
        } else if (Double.class.getName().equals(clazz) || double.class.getName().equals(clazz)) {
            return PulsarSchema.float64().supplierV4();
        } else if (String.class.getName().equals(clazz)) {
            return PulsarSchema.string().supplierV4();
        } else if (HazelcastJsonValue.class.getName().equals(clazz)) {
            return PulsarJsonSchema::new;
        } else {
            return null;
        }
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

    @Override
    public PlanObjectKey getObjectKey() {
        return new PulsarPlanObjectKey(getSchemaName(), getSqlName(), topicName(), getFields(), options());
    }

    String topicName() {
        return topicName;
    }

    String dataConnectionName() {
        return dataConnectionName;
    }

    QueryPath[] paths() {
        return getFields().stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
    }

    QueryDataType[] types() {
        return getFields().stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    record PulsarPlanObjectKey(
        String schemaName,
        String tableName,
        String topicName,
        List<TableField> fields,
        Map<String, String> options) implements PlanObjectKey {

    }

    int preferredLocalParallelism() {
        return Integer.parseInt(options.getOrDefault(OPTION_PREFERRED_LOCAL_PARALLELISM,
                                                     String.valueOf(Vertex.LOCAL_PARALLELISM_USE_DEFAULT)));
    }

    String getTopicName() {
        return topicName;
    }

    Map<String, String> options() {
        return options;
    }

    @Override
    public String toString() {
        return "Pulsar [" + TopicName.get(topicName) + ']';
    }
}

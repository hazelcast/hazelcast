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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.kafka.impl.StreamKafkaP;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJsonResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataNullResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class KafkaSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "Kafka";

    private static final KvMetadataResolvers METADATA_RESOLVERS = new KvMetadataResolvers(
            new KvMetadataResolver[]{
                    KvMetadataNullResolver.INSTANCE,
                    KvMetadataJavaResolver.INSTANCE,
                    KvMetadataJsonResolver.INSTANCE,
                    KvMetadataAvroResolver.INSTANCE
            },
            new KvMetadataResolver[]{
                    KvMetadataJavaResolver.INSTANCE,
                    KvMetadataJsonResolver.INSTANCE,
                    KvMetadataAvroResolver.INSTANCE
            }
    );

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
        return METADATA_RESOLVERS.resolveAndValidateFields(userFields, options, nodeEngine);
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
        KvMetadata keyMetadata = METADATA_RESOLVERS.resolveMetadata(true, resolvedFields, options, null);
        KvMetadata valueMetadata = METADATA_RESOLVERS.resolveMetadata(false, resolvedFields, options, null);
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        return new KafkaTable(
                this,
                schemaName,
                mappingName,
                fields,
                new ConstantTableStatistics(0),
                externalName,
                options,
                keyMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor()
        );
    }

    @Nonnull @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        KafkaTable table = (KafkaTable) table0;

        return dag.newUniqueVertex(
                table.toString(),
                ProcessorMetaSupplier.of(
                        StreamKafkaP.PREFERRED_LOCAL_PARALLELISM,
                        new RowProjectorProcessorSupplier(
                                table.kafkaConsumerProperties(),
                                table.topicName(),
                                eventTimePolicyProvider,
                                table.paths(),
                                table.types(),
                                table.keyQueryDescriptor(),
                                table.valueQueryDescriptor(),
                                predicate,
                                projections
                        )
                )
        );
    }

    @Nonnull @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DAG dag, @Nonnull Table table) {
        return new VertexWithInputConfig(writeProcessor(dag, table));
    }

    @Nonnull @Override
    public Vertex sinkProcessor(@Nonnull DAG dag, @Nonnull Table table) {
        return writeProcessor(dag, table);
    }

    @Nonnull
    private Vertex writeProcessor(DAG dag, Table table0) {
        KafkaTable table = (KafkaTable) table0;

        Vertex vStart = dag.newUniqueVertex(
                "Project(" + table + ")",
                KvProcessors.entryProjector(
                        table.paths(),
                        table.types(),
                        table.keyUpsertDescriptor(),
                        table.valueUpsertDescriptor(),
                        false
                )
        );
        // set the parallelism to match that of the kafka sink - see https://github.com/hazelcast/hazelcast/issues/20507
        // TODO eliminate the project vertex altogether and do the projecting in the sink directly
        vStart.localParallelism(1);

        Vertex vEnd = dag.newUniqueVertex(
                table.toString(),
                KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                        table.kafkaProducerProperties(),
                        table.topicName(),
                        Entry::getKey,
                        Entry::getValue,
                        true
                )
        );

        dag.edge(between(vStart, vEnd));
        return vStart;
    }
}

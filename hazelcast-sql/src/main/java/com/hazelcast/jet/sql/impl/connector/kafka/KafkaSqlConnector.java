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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.kafka.impl.StreamKafkaP;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJsonResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataNullResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.sql.impl.QueryUtils.quoteCompoundIdentifier;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class KafkaSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "Kafka";
    public static final String OPTION_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String OPTION_OFFSET_RESET = "auto.offset.reset";

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

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "Topic";
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        if (externalResource.externalName().length > 1) {
            throw QueryException.error("Invalid external name " + quoteCompoundIdentifier(externalResource.externalName())
                    + ", external name for Kafka is allowed to have only a single component referencing the topic " +
                    "name");
        }
        return METADATA_RESOLVERS.resolveAndValidateFields(userFields, externalResource.options(), nodeEngine);
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        KvMetadata keyMetadata = METADATA_RESOLVERS.resolveMetadata(true, resolvedFields, externalResource.options(), null);
        KvMetadata valueMetadata = METADATA_RESOLVERS.resolveMetadata(false, resolvedFields, externalResource.options(), null);
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        return new KafkaTable(
                this,
                schemaName,
                mappingName,
                fields,
                new ConstantTableStatistics(0),
                externalResource.externalName()[0],
                externalResource.dataConnection(),
                externalResource.options(),
                keyMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                externalResource.objectType()
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
        KafkaTable table = context.getTable();

        return context.getDag().newUniqueVertex(
                table.toString(),
                ProcessorMetaSupplier.of(
                        StreamKafkaP.PREFERRED_LOCAL_PARALLELISM,
                        new RowProjectorProcessorSupplier(
                                table.kafkaConsumerProperties(),
                                table.dataConnectionName(),
                                table.topicName(),
                                eventTimePolicyProvider,
                                table.paths(),
                                table.types(),
                                table.keyQueryDescriptor(),
                                table.valueQueryDescriptor(),
                                context.convertFilter(predicate),
                                context.convertProjection(projection)
                        )
                )
        );
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        return new VertexWithInputConfig(writeProcessor(context));
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        return writeProcessor(context);
    }

    @Override
    public boolean supportsExpression(@Nonnull HazelcastRexNode expression) {
        return true;
    }

    @Nonnull
    private Vertex writeProcessor(@Nonnull DagBuildContext context) {
        KafkaTable table = context.getTable();

        Vertex vStart = context.getDag().newUniqueVertex(
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
        // TODO: eliminate the project vertex altogether and do the projecting in the sink directly
        vStart.localParallelism(1);

        Vertex vEnd = context.getDag().newUniqueVertex(
                table.toString(),
                table.dataConnectionName() == null
                        ?
                        KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                                table.kafkaProducerProperties(),
                                table.topicName(),
                                Entry::getKey,
                                Entry::getValue,
                                true)
                        :
                        KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                                new DataConnectionRef(table.dataConnectionName()),
                                table.kafkaProducerProperties(),
                                table.topicName(),
                                Entry::getKey,
                                Entry::getValue,
                                true)
        );

        context.getDag().edge(between(vStart, vEnd));
        return vStart;
    }

    @Override
    public Set<String> nonSensitiveConnectorOptions() {
        Set<String> set = SqlConnector.super.nonSensitiveConnectorOptions();
        set.addAll(Set.of(
                OPTION_BOOTSTRAP_SERVERS,
                OPTION_OFFSET_RESET
        ));
        return set;
    }
}

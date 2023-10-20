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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.config.BitmapIndexOptions;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.AlterJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateIndexPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateTypePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateViewPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DmlPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropMappingPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropTypePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropViewPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.ExplainStatementPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapDeletePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapInsertPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapSelectPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapSinkPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapUpdatePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.SelectPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.ShowStatementPlan;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.TypeDefinitionColumn;
import com.hazelcast.jet.sql.impl.schema.TypesUtils;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EntryRemovingProcessor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.UpdateSqlResultImpl;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.sql.impl.state.QueryResultRegistry;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import static com.hazelcast.jet.config.JobConfigArguments.KEY_SQL_QUERY_TEXT;
import static com.hazelcast.jet.config.JobConfigArguments.KEY_SQL_UNBOUNDED;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologyException;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.impl.util.Util.getSerializationService;
import static com.hazelcast.jet.sql.impl.parse.SqlCreateIndex.UNIQUE_KEY;
import static com.hazelcast.jet.sql.impl.parse.SqlCreateIndex.UNIQUE_KEY_TRANSFORMATION;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static com.hazelcast.sql.impl.expression.ExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;

public class PlanExecutor {
    private static final String LE = System.lineSeparator();
    private static final String DEFAULT_UNIQUE_KEY = "__key";
    private static final String DEFAULT_UNIQUE_KEY_TRANSFORMATION = "OBJECT";

    private final TableResolverImpl catalog;
    private final HazelcastInstance hazelcastInstance;
    private final QueryResultRegistry resultRegistry;

    public PlanExecutor(
            TableResolverImpl catalog,
            HazelcastInstance hazelcastInstance,
            QueryResultRegistry resultRegistry
    ) {
        this.catalog = catalog;
        this.hazelcastInstance = hazelcastInstance;
        this.resultRegistry = resultRegistry;
    }

    SqlResult execute(CreateMappingPlan plan, SqlSecurityContext ssc) {
        catalog.createMapping(plan.mapping(), plan.replace(), plan.ifNotExists(), ssc);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropMappingPlan plan) {
        catalog.removeMapping(plan.name(), plan.ifExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateIndexPlan plan) {
        if (!plan.ifNotExists()) {
            // If `IF NOT EXISTS` isn't specified, we do a simple check for the existence of the index. This is not
            // OK if two clients concurrently try to create the index (they could both succeed), but covers the
            // common case. There's no atomic operation to create an index in IMDG, so it's not easy to
            // implement.
            MapContainer mapContainer = getMapContainer(hazelcastInstance.getMap(plan.mapName()));

            if (mapContainer.getIndexes().getIndex(plan.indexName()) != null) {
                throw QueryException.error("Can't create index: index '" + plan.indexName() + "' already exists");
            }
        }

        IndexConfig indexConfig = new IndexConfig(plan.indexType(), plan.attributes())
                .setName(plan.indexName());

        if (plan.indexType().equals(IndexType.BITMAP)) {
            Map<String, String> options = plan.options();

            String uniqueKey = options.get(UNIQUE_KEY);
            if (uniqueKey == null) {
                uniqueKey = DEFAULT_UNIQUE_KEY;
            }

            String uniqueKeyTransform = options.get(UNIQUE_KEY_TRANSFORMATION);
            if (uniqueKeyTransform == null) {
                uniqueKeyTransform = DEFAULT_UNIQUE_KEY_TRANSFORMATION;
            }

            BitmapIndexOptions bitmapIndexOptions = new BitmapIndexOptions();
            bitmapIndexOptions.setUniqueKey(uniqueKey);
            bitmapIndexOptions.setUniqueKeyTransformation(UniqueKeyTransformation.fromName(uniqueKeyTransform));

            indexConfig.setBitmapIndexOptions(bitmapIndexOptions);
        }

        // The `addIndex()` call does nothing, if an index with the same name already exists.
        // Even if its config is different.
        hazelcastInstance.getMap(plan.mapName()).addIndex(indexConfig);

        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateJobPlan plan, List<Object> arguments) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = plan.getJobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setArgument(KEY_SQL_QUERY_TEXT, plan.getQuery())
                .setArgument(KEY_SQL_UNBOUNDED, plan.isInfiniteRows());
        if (plan.isIfNotExists()) {
            hazelcastInstance.getJet().newJobIfAbsent(plan.getExecutionPlan().getDag(), jobConfig);
        } else {
            hazelcastInstance.getJet().newJob(plan.getExecutionPlan().getDag(), jobConfig);
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(AlterJobPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
        if (job == null) {
            throw QueryException.error("The job '" + plan.getJobName() + "' doesn't exist");
        }
        switch (plan.getOperation()) {
            case SUSPEND:
                job.suspend();
                break;

            case RESUME:
                job.resume();
                break;

            case RESTART:
                job.restart();
                break;

            default:
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropJobPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
        boolean jobTerminated = job != null && job.getStatus().isTerminal();
        if (job == null || jobTerminated) {
            if (plan.isIfExists()) {
                return UpdateSqlResultImpl.createUpdateCountResult(0);
            }
            if (jobTerminated) {
                throw QueryException.error("Job already terminated: " + plan.getJobName());
            } else {
                throw QueryException.error("Job doesn't exist: " + plan.getJobName());
            }
        }
        if (plan.getWithSnapshotName() != null) {
            job.cancelAndExportSnapshot(plan.getWithSnapshotName());
        } else {
            job.cancel();
        }
        try {
            job.join();
        } catch (CancellationException ignored) {
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateSnapshotPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
        if (job == null) {
            throw QueryException.error("The job '" + plan.getJobName() + "' doesn't exist");
        }
        job.exportSnapshot(plan.getSnapshotName());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropSnapshotPlan plan) {
        JobStateSnapshot snapshot = hazelcastInstance.getJet().getJobStateSnapshot(plan.getSnapshotName());
        if (snapshot == null) {
            if (plan.isIfExists()) {
                return UpdateSqlResultImpl.createUpdateCountResult(0);
            }
            throw QueryException.error("The snapshot doesn't exist: " + plan.getSnapshotName());
        }
        snapshot.destroy();
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateViewPlan plan, SqlSecurityContext ssc) {
        OptimizerContext context = plan.context();
        SqlNode sqlNode = context.parse(plan.viewQuery(), ssc).getNode();
        RelNode relNode = context.convert(sqlNode).getRel();
        List<RelDataTypeField> fieldList = relNode.getRowType().getFieldList();

        List<String> fieldNames = new ArrayList<>();
        List<QueryDataType> fieldTypes = new ArrayList<>();

        for (RelDataTypeField field : fieldList) {
            fieldNames.add(field.getName());
            fieldTypes.add(toHazelcastType(field.getType()));
        }

        View view = new View(plan.viewName(), plan.viewQuery(), fieldNames, fieldTypes);
        catalog.createView(view, plan.isReplace(), plan.ifNotExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropViewPlan plan) {
        catalog.removeView(plan.viewName(), plan.isIfExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropTypePlan plan) {
        catalog.removeType(plan.typeName(), plan.isIfExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(ShowStatementPlan plan) {
        Stream<String> rows;

        switch (plan.getShowTarget()) {
            case MAPPINGS:
                rows = catalog.getMappingNames().stream();
                break;
            case VIEWS:
                rows = catalog.getViewNames().stream();
                break;
            case JOBS:
                NodeEngine nodeEngine = getNodeEngine(hazelcastInstance);
                JetServiceBackend jetServiceBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
                rows = jetServiceBackend.getJobRepository().getActiveJobNames().stream();
                break;
            case TYPES:
                rows = catalog.getTypeNames().stream();
                break;
            default:
                throw new AssertionError("Unsupported SHOW statement target");
        }
        SqlRowMetadata metadata = new SqlRowMetadata(singletonList(new SqlColumnMetadata("name", VARCHAR, false)));
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);

        return new SqlResultImpl(
                QueryId.create(hazelcastInstance.getLocalEndpoint().getUuid()),
                new StaticQueryResultProducerImpl(
                        rows.sorted().map(name -> new JetSqlRow(serializationService, new Object[]{name})).iterator()),
                metadata,
                false
        );
    }

    SqlResult execute(ExplainStatementPlan plan) {
        Stream<String> planRows;
        SqlRowMetadata metadata = new SqlRowMetadata(
                singletonList(
                        new SqlColumnMetadata("rel", VARCHAR, false)
                )
        );
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);

        planRows = Arrays.stream(plan.getRel().explain().split(LE));
        return new SqlResultImpl(
                QueryId.create(hazelcastInstance.getLocalEndpoint().getUuid()),
                new StaticQueryResultProducerImpl(
                        planRows.map(rel -> new JetSqlRow(serializationService, new Object[]{rel})).iterator()),
                metadata,
                false
        );
    }

    SqlResult execute(SelectPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setArgument(KEY_SQL_QUERY_TEXT, plan.getQuery())
                .setArgument(KEY_SQL_UNBOUNDED, plan.isStreaming())
                .setTimeoutMillis(timeout);

        QueryResultProducerImpl queryResultProducer = new QueryResultProducerImpl(!plan.isStreaming());
        AbstractJetInstance<?> jet = (AbstractJetInstance<?>) hazelcastInstance.getJet();
        long jobId = jet.newJobId();
        Object oldValue = resultRegistry.store(jobId, queryResultProducer);
        assert oldValue == null : oldValue;
        try {
            Job job = jet.newLightJob(jobId, plan.getDag(), jobConfig);
            job.getFuture().whenComplete((r, t) -> {
                // make sure the queryResultProducer is cleaned up after the job completes. This normally
                // takes effect when the job fails before the QRP is removed by the RootResultConsumerSink
                resultRegistry.remove(jobId);
                if (t != null) {
                    int errorCode = findQueryExceptionCode(t);
                    String errorMessage = findQueryExceptionMessage(t);
                    queryResultProducer.onError(QueryException.error(errorCode, "The Jet SQL job failed: " + errorMessage, t));
                }
            });
        } catch (Throwable e) {
            resultRegistry.remove(jobId);
            throw e;
        }

        return new SqlResultImpl(
                queryId,
                queryResultProducer,
                plan.getRowMetadata(),
                plan.isStreaming()
        );
    }

    SqlResult execute(DmlPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setArgument(KEY_SQL_QUERY_TEXT, plan.getQuery())
                .setArgument(KEY_SQL_UNBOUNDED, plan.isInfiniteRows())
                .setTimeoutMillis(timeout);

        Job job = hazelcastInstance.getJet().newLightJob(plan.getDag(), jobConfig);
        job.join();

        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapSelectPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);
        ExpressionEvalContext evalContext = new ExpressionEvalContext(args, serializationService);
        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<JetSqlRow> future = hazelcastInstance.getMap(plan.mapName())
                .getAsync(key)
                .toCompletableFuture()
                .thenApply(value -> value == null ? null : plan.rowProjectorSupplier()
                        .get(evalContext, Extractors.newBuilder(serializationService).build())
                        .project(key, value));
        JetSqlRow row = await(future, timeout);
        StaticQueryResultProducerImpl resultProducer = row != null
                ? new StaticQueryResultProducerImpl(row)
                : new StaticQueryResultProducerImpl(emptyIterator());
        return new SqlResultImpl(queryId, resultProducer, plan.rowMetadata(), false);
    }

    SqlResult execute(IMapInsertPlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = new ExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        List<Entry<Object, Object>> entries = plan.entriesFn().apply(evalContext);
        if (!entries.isEmpty()) {
            assert entries.size() == 1;
            Entry<Object, Object> entry = entries.get(0);
            CompletableFuture<Object> future = ((MapProxyImpl<Object, Object>) hazelcastInstance.getMap(plan.mapName()))
                    .putIfAbsentAsync(entry.getKey(), entry.getValue())
                    .toCompletableFuture();
            Object previous = await(future, timeout);
            if (previous != null) {
                throw QueryException.error("Duplicate key");
            }
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapSinkPlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = new ExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        Map<Object, Object> entries = plan.entriesFn().apply(evalContext);
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .putAllAsync(entries)
                .toCompletableFuture();
        await(future, timeout);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapUpdatePlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = new ExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<Long> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, plan.updaterSupplier().get(arguments))
                .toCompletableFuture();
        await(future, timeout);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapDeletePlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = new ExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR)
                .toCompletableFuture();
        await(future, timeout);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateTypePlan plan) {
        NodeEngineImpl nodeEngine = getNodeEngine(hazelcastInstance);
        if (!nodeEngine.getProperties().getBoolean(SQL_CUSTOM_TYPES_ENABLED)) {
            throw QueryException.error("Experimental feature of creating custom types isn't enabled. To enable, set "
                    + SQL_CUSTOM_TYPES_ENABLED + " to true");
        }
        final String format = plan.options().get(SqlConnector.OPTION_FORMAT);
        final Type type;

        if (SqlConnector.PORTABLE_FORMAT.equals(format)) {
            final Integer factoryId = Optional.ofNullable(plan.option(SqlConnector.OPTION_TYPE_PORTABLE_FACTORY_ID))
                    .map(Integer::parseInt)
                    .orElse(null);
            final Integer classId = Optional.ofNullable(plan.option(SqlConnector.OPTION_TYPE_PORTABLE_CLASS_ID))
                    .map(Integer::parseInt)
                    .orElse(null);
            final Integer version = Optional.ofNullable(plan.option(SqlConnector.OPTION_TYPE_PORTABLE_CLASS_VERSION))
                    .map(Integer::parseInt)
                    .orElse(0);

            if (factoryId == null || classId == null) {
                throw QueryException.error("FactoryID and ClassID are required for Portable Types");
            }

            final ClassDefinition existingClassDef = getSerializationService(hazelcastInstance).getPortableContext()
                    .lookupClassDefinition(factoryId, classId, version);

            if (existingClassDef != null) {
                type = TypesUtils.convertPortableClassToType(plan.name(), existingClassDef, catalog);
            } else {
                if (plan.columns().isEmpty()) {
                    throw QueryException.error("The given FactoryID/ClassID/Version combination not known to the member. " +
                            "You need to provide column list for this type");
                }

                type = new Type();
                type.setName(plan.name());
                type.setKind(TypeKind.PORTABLE);
                type.setPortableFactoryId(factoryId);
                type.setPortableClassId(classId);
                type.setPortableVersion(version);
                type.setFields(new ArrayList<>());

                for (int i = 0; i < plan.columns().size(); i++) {
                    final TypeDefinitionColumn planColumn = plan.columns().get(i);
                    type.getFields().add(new Type.TypeField(planColumn.name(), planColumn.dataType()));
                }
            }
        } else if (SqlConnector.COMPACT_FORMAT.equals(format)) {
            if (plan.columns().isEmpty()) {
                throw QueryException.error("Column list is required to create Compact-based Types");
            }
            type = new Type();
            type.setKind(TypeKind.COMPACT);
            type.setName(plan.name());
            final List<Type.TypeField> typeFields = plan.columns().stream()
                    .map(typeColumn -> new Type.TypeField(typeColumn.name(), typeColumn.dataType()))
                    .collect(Collectors.toList());
            type.setFields(typeFields);

            final String compactTypeName = plan.option(SqlConnector.OPTION_TYPE_COMPACT_TYPE_NAME);
            if (compactTypeName == null || compactTypeName.isEmpty()) {
                throw QueryException.error("Compact Type Name must not be empty for Compact-based Types.");
            }
            type.setCompactTypeName(compactTypeName);
        } else if (SqlConnector.JAVA_FORMAT.equals(format)) {
            final Class<?> typeClass;
            try {
                typeClass = ReflectionUtils.loadClass(plan.options().get(SqlConnector.OPTION_TYPE_JAVA_CLASS));
            } catch (Exception e) {
                throw QueryException.error("Unable to load class: '"
                        + plan.options().get(SqlConnector.OPTION_TYPE_JAVA_CLASS) + "'", e);
            }

            type = TypesUtils.convertJavaClassToType(plan.name(), plan.columns(), typeClass);
        } else {
            throw QueryException.error("Unsupported type format: " + format);
        }

        catalog.createType(type, plan.replace(), plan.ifNotExists());

        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    private List<Object> prepareArguments(QueryParameterMetadata parameterMetadata, List<Object> arguments) {
        assert arguments != null;

        int parameterCount = parameterMetadata.getParameterCount();
        if (parameterCount != arguments.size()) {
            throw QueryException.error(
                    SqlErrorCode.DATA_EXCEPTION,
                    "Unexpected parameter count: expected " + parameterCount + ", got " + arguments.size()
            );
        }

        for (int i = 0; i < arguments.size(); ++i) {
            Object value = arguments.get(i);

            ParameterConverter parameterConverter = parameterMetadata.getParameterConverter(i);

            Object newValue = parameterConverter.convert(value);

            if (newValue != value) {
                arguments.set(i, newValue);
            }
        }

        return arguments;
    }

    private static int findQueryExceptionCode(Throwable t) {
        while (t != null) {
            if (t instanceof QueryException) {
                return ((QueryException) t).getCode();
            }
            if (isTopologyException(t)) {
                return SqlErrorCode.TOPOLOGY_CHANGE;
            }
            if (t instanceof RestartableException) {
                return SqlErrorCode.RESTARTABLE_ERROR;
            }
            t = t.getCause();
        }

        return SqlErrorCode.GENERIC;
    }

    private static String findQueryExceptionMessage(Throwable t) {
        while (t != null) {
            if (t.getMessage() != null) {
                return t.getMessage();
            }
            t = t.getCause();
        }
        return "";
    }

    private <T> T await(CompletableFuture<T> future, long timeout) {
        try {
            return timeout > 0 ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
        } catch (TimeoutException e) {
            future.cancel(true);
            throw QueryException.error("Timeout occurred while executing statement");
        } catch (InterruptedException | ExecutionException e) {
            throw QueryException.error(e.getMessage(), e);
        }
    }

    private static <K, V> MapContainer getMapContainer(IMap<K, V> map) {
        MapProxyImpl<K, V> mapProxy = (MapProxyImpl<K, V>) map;
        MapService mapService = mapProxy.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(map.getName());
    }
}

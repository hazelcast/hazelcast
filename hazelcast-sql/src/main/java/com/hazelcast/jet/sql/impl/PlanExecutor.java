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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.BitmapIndexOptions;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.impl.DataConnectionServiceImpl;
import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.PartitioningStrategyUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.JobConfigArguments;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.AlterJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateIndexPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateTypePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateViewPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DmlPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropDataConnectionPlan;
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
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget;
import com.hazelcast.jet.sql.impl.schema.DataConnectionResolver;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.validate.UpdateDataConnectionOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EntryRemovingProcessor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.UpdateSqlResultImpl;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.sql.impl.state.QueryResultRegistry;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import static com.hazelcast.jet.config.JobConfigArguments.KEY_SQL_QUERY_TEXT;
import static com.hazelcast.jet.config.JobConfigArguments.KEY_SQL_UNBOUNDED;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologyException;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateDataConnectionPlan;
import static com.hazelcast.jet.sql.impl.parse.SqlCreateIndex.UNIQUE_KEY;
import static com.hazelcast.jet.sql.impl.parse.SqlCreateIndex.UNIQUE_KEY_TRANSFORMATION;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.sql.SqlColumnType.JSON;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static com.hazelcast.sql.impl.QueryUtils.quoteCompoundIdentifier;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;

public class PlanExecutor {
    private static final String LE = System.lineSeparator();
    private static final String DEFAULT_UNIQUE_KEY_TRANSFORMATION = "OBJECT";

    private final TableResolverImpl catalog;
    private final DataConnectionResolver dataConnectionCatalog;
    private final HazelcastInstance hazelcastInstance;
    private final NodeEngine nodeEngine;
    private final QueryResultRegistry resultRegistry;
    private final List<SqlJobInvocationObserver> sqlJobInvocationObservers = new ArrayList<>();

    private final ILogger logger;

    // test-only
    private final AtomicLong directIMapQueriesExecuted = new AtomicLong();

    public PlanExecutor(
            NodeEngine nodeEngine,
            TableResolverImpl catalog,
            DataConnectionResolver dataConnectionResolver,
            QueryResultRegistry resultRegistry
    ) {
        this.nodeEngine = nodeEngine;
        this.hazelcastInstance = nodeEngine.getHazelcastInstance();
        this.catalog = catalog;
        this.dataConnectionCatalog = dataConnectionResolver;
        this.resultRegistry = resultRegistry;

        logger = nodeEngine.getLogger(getClass());
    }

    SqlResult execute(CreateMappingPlan plan, SqlSecurityContext ssc) {
        catalog.createMapping(plan.mapping(), plan.replace(), plan.ifNotExists(), ssc);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropMappingPlan plan) {
        catalog.removeMapping(plan.name(), plan.ifExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateDataConnectionPlan plan) {
        InternalDataConnectionService dlService = nodeEngine.getDataConnectionService();
        assert !plan.ifNotExists() || !plan.isReplace();

        if (dlService.existsConfigDataConnection(plan.name())) {
            throw new HazelcastException("Cannot replace a data connection created from configuration");
        }

        // checks if type is correct
        dlService.classForDataConnectionType(plan.type());

        boolean added = dataConnectionCatalog.createDataConnection(
                new DataConnectionCatalogEntry(
                        plan.name(),
                        plan.type().toLowerCase(Locale.ROOT),
                        plan.shared(),
                        plan.options()),
                plan.isReplace(),
                plan.ifNotExists());
        if (added) {
            broadcastUpdateDataConnectionOperations(plan.name());
            // TODO invoke the listeners so plans can be invalidated after the
            //  change was propagated to InternalDataConnectionService
            dataConnectionCatalog.invokeChangeListeners();
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropDataConnectionPlan plan) {
        InternalDataConnectionService dlService = nodeEngine.getDataConnectionService();
        if (dlService.existsConfigDataConnection(plan.name())) {
            throw new HazelcastException("Data connection '" + plan.name() + "' is configured via Config and can't be removed");
        }
        dataConnectionCatalog.removeDataConnection(plan.name(), plan.ifExists());
        broadcastUpdateDataConnectionOperations(plan.name());
        // TODO invoke the listeners so plans can be invalidated after the
        //  change was propagated to InternalDataConnectionService
        dataConnectionCatalog.invokeChangeListeners();
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateIndexPlan plan) {
        MapContainer mapContainer = getMapContainer(hazelcastInstance.getMap(plan.mapName()));
        if (!mapContainer.shouldUseGlobalIndex()) {
            // for partitioned indexes checking existence is more complicated
            // and SQL cannot yet use partitioned indexes
            throw QueryException.error(SqlErrorCode.INDEX_INVALID, "Cannot create index \"" + plan.indexName()
                    + "\" on the IMap \"" + plan.mapName() + "\" because it would not be global "
                    + "(make sure the property \"" + ClusterProperty.GLOBAL_HD_INDEX_ENABLED
                    + "\" is set to \"true\")");
        }

        if (!plan.ifNotExists()) {
            // If `IF NOT EXISTS` isn't specified, we do a simple check for the existence of the index. This is not
            // OK if two clients concurrently try to create the index (they could both succeed), but covers the
            // common case. There's no atomic operation to create an index in IMDG, so it's not easy to implement.
            if (mapContainer.getGlobalIndexRegistry().getIndex(plan.indexName()) != null) {
                throw QueryException.error("Can't create index: index '" + plan.indexName() + "' already exists");
            }
        }

        IndexConfig indexConfig = new IndexConfig(plan.indexType(), plan.attributes())
                .setName(plan.indexName());

        if (plan.indexType().equals(IndexType.BITMAP)) {
            Map<String, String> options = plan.options();

            String uniqueKey = options.get(UNIQUE_KEY);
            if (uniqueKey == null) {
                uniqueKey = KEY_ATTRIBUTE_NAME.value();
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

    SqlResult execute(CreateJobPlan plan, List<Object> arguments, SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        boolean isStreamingJob = plan.isInfiniteRows();
        JobConfig jobConfig = plan.getJobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setArgument(KEY_SQL_QUERY_TEXT, plan.getQuery())
                .setArgument(KEY_SQL_UNBOUNDED, isStreamingJob);
        if (!jobConfig.isSuspendOnFailure()) {
            jobConfig.setSuspendOnFailure(isStreamingJob);
        }

        AbstractJetInstance<?> jet = (AbstractJetInstance<?>) hazelcastInstance.getJet();

        if (plan.isIfNotExists()) {
            jet.newJobIfAbsent(plan.getExecutionPlan().getDag(), jobConfig, ssc.subject());
        } else {
            jet.newJob(plan.getExecutionPlan().getDag(), jobConfig, ssc.subject());
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(AlterJobPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
        if (job == null) {
            throw QueryException.error("The job '" + plan.getJobName() + "' doesn't exist");
        }

        assert plan.getDeltaConfig() != null || plan.getOperation() != null;
        if (plan.getDeltaConfig() != null) {
            try {
                job.updateConfig(plan.getDeltaConfig());
            } catch (IllegalStateException e) {
                throw QueryException.error(e.getMessage(), e);
            }
        }
        if (plan.getOperation() != null) {
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

    SqlResult execute(CreateViewPlan plan) {
        OptimizerContext context = plan.context();
        SqlNode sqlNode = context.parse(plan.viewQuery()).getNode();
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    SqlResult execute(ShowStatementPlan plan) {
        Stream<List<?>> rows;

        switch (plan.getShowTarget()) {
            case MAPPINGS:
                rows = catalog.getMappingNames().stream().map(Collections::singletonList);
                break;
            case VIEWS:
                rows = catalog.getViewNames().stream().map(Collections::singletonList);
                break;
            case JOBS:
                JetServiceBackend jetServiceBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
                rows = jetServiceBackend.getJobRepository().getActiveJobNames().stream().map(Collections::singletonList);
                break;
            case TYPES:
                rows = catalog.getTypeNames().stream().map(Collections::singletonList);
                break;
            case DATACONNECTIONS:
                InternalDataConnectionService service = nodeEngine.getDataConnectionService();
                DataConnectionServiceImpl dataConnectionService = (DataConnectionServiceImpl) service;
                rows = DataConnectionResolver
                        .getAllDataConnectionNameWithTypes(dataConnectionService)
                        .stream();
                break;
            case RESOURCES:
                return executeShowResources(plan.getDataConnectionName());
            default:
                throw new AssertionError("Unsupported SHOW statement target");
        }
        SqlRowMetadata metadata =
                plan.getShowTarget() == ShowStatementTarget.DATACONNECTIONS
                        ? new SqlRowMetadata(asList(
                        new SqlColumnMetadata("name", VARCHAR, false),
                        new SqlColumnMetadata("connection_type", VARCHAR, false),
                        new SqlColumnMetadata("resource_types", JSON, false)
                ))
                        : new SqlRowMetadata(singletonList(new SqlColumnMetadata("name", VARCHAR, false)));
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);

        return new SqlResultImpl(
                QueryId.create(hazelcastInstance.getLocalEndpoint().getUuid()),
                new StaticQueryResultProducerImpl(
                        rows.sorted(comparing(r -> (Comparable) r.get(0)))
                                .map(row -> new JetSqlRow(serializationService, ((List<?>) row).toArray(new Object[0])))
                                .iterator()),
                metadata,
                false
        );
    }

    private SqlResult executeShowResources(@Nullable String dataConnectionName) {
        if (dataConnectionName == null) {
            throw QueryException.error("Data connections exist only in the 'public' schema");
        }

        final SqlRowMetadata metadata = new SqlRowMetadata(asList(
                new SqlColumnMetadata("name", VARCHAR, false),
                new SqlColumnMetadata("type", VARCHAR, false)
        ));
        final InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);
        final InternalDataConnectionService dataConnectionService = getNodeEngine(hazelcastInstance).getDataConnectionService();

        final List<JetSqlRow> rows;
        final DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(
                dataConnectionName, DataConnection.class);
        try {
            rows = dataConnection.listResources().stream()
                    .map(resource -> new JetSqlRow(
                            serializationService,
                            new Object[]{
                                    quoteCompoundIdentifier(resource.name()),
                                    resource.type()
                            }
                    ))
                    .collect(Collectors.toList());
        } finally {
            dataConnection.release();
        }

        return new SqlResultImpl(
                QueryId.create(hazelcastInstance.getLocalEndpoint().getUuid()),
                new StaticQueryResultProducerImpl(rows.iterator()),
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

    SqlResult execute(SelectPlan plan,
                      QueryId queryId,
                      List<Object> arguments,
                      long timeout,
                      @Nonnull SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        ExpressionEvalContext evalContext = ExpressionEvalContext.createContext(
                args,
                hazelcastInstance,
                Util.getSerializationService(hazelcastInstance),
                ssc
        );

        JobConfig jobConfig = plan.isAnalyzed() ? plan.analyzeJobConfig() : new JobConfig();
        jobConfig.setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setArgument(KEY_SQL_QUERY_TEXT, plan.getQuery())
                .setArgument(KEY_SQL_UNBOUNDED, plan.isStreaming())
                .setTimeoutMillis(timeout);

        if (!plan.getPartitionStrategyCandidates().isEmpty()) {
            final Set<Integer> partitions = tryUsePrunability(plan, evalContext);
            if (!partitions.isEmpty()) {
                jobConfig.setArgument(JobConfigArguments.KEY_REQUIRED_PARTITIONS, partitions);
            }
        }

        QueryResultProducerImpl queryResultProducer = new QueryResultProducerImpl(!plan.isStreaming());
        AbstractJetInstance<?> jet = (AbstractJetInstance<?>) hazelcastInstance.getJet();
        long jobId = jet.newJobId();
        Object oldValue = resultRegistry.store(jobId, queryResultProducer);
        assert oldValue == null : oldValue;
        try {
            sqlJobInvocationObservers.forEach(observer -> observer.onJobInvocation(plan.getDag(), jobConfig));
            Job job = plan.isAnalyzed()
                    ? jet.newJob(jobId, plan.getDag(), jobConfig, ssc.subject())
                    : jet.newLightJob(jobId, plan.getDag(), jobConfig, ssc.subject());

            job.getFuture().whenComplete((r, t) -> {
                // make sure the queryResultProducer is cleaned up after the job completes. This normally
                // takes effect when the job fails before the QRP is removed by the RootResultConsumerSink
                resultRegistry.remove(jobId);
                if (t != null) {
                    int errorCode = findQueryExceptionCode(t);
                    String errorMessage = findQueryExceptionMessage(t);
                    queryResultProducer.onError(
                            QueryException.error(errorCode, "The Jet SQL job failed: " + errorMessage, t));
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

    SqlResult execute(DmlPlan plan,
                      QueryId queryId,
                      List<Object> arguments,
                      long timeout,
                      @Nonnull SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setArgument(KEY_SQL_QUERY_TEXT, plan.getQuery())
                .setArgument(KEY_SQL_UNBOUNDED, plan.isInfiniteRows())
                .setTimeoutMillis(timeout);

        AbstractJetInstance<?> jet = (AbstractJetInstance<?>) hazelcastInstance.getJet();
        sqlJobInvocationObservers.forEach(observer -> observer.onJobInvocation(plan.getDag(), jobConfig));
        Job job = plan.isAnalyzed()
                ? jet.newJob(plan.getDag(), jobConfig, ssc.subject())
                : jet.newLightJob(plan.getDag(), jobConfig, ssc.subject());
        job.join();

        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapSelectPlan plan,
                      QueryId queryId,
                      List<Object> arguments,
                      long timeout,
                      @Nonnull SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);
        ExpressionEvalContext evalContext = ExpressionEvalContext.createContext(
                args,
                hazelcastInstance,
                serializationService,
                ssc
        );

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

        directIMapQueriesExecuted.getAndIncrement();

        return new SqlResultImpl(
                queryId,
                resultProducer,
                plan.rowMetadata(),
                false,
                plan.keyConditionParamIndex()
        );
    }

    SqlResult execute(IMapInsertPlan plan, List<Object> arguments, long timeout, SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = ExpressionEvalContext.createContext(
                args,
                hazelcastInstance,
                Util.getSerializationService(hazelcastInstance),
                ssc
        );

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

        directIMapQueriesExecuted.getAndIncrement();

        return UpdateSqlResultImpl.createUpdateCountResult(0, plan.keyParamIndex());
    }

    SqlResult execute(IMapSinkPlan plan, List<Object> arguments, long timeout, @Nonnull SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = ExpressionEvalContext.createContext(
                args,
                hazelcastInstance,
                Util.getSerializationService(hazelcastInstance),
                ssc
        );

        Map<Object, Object> entries = plan.entriesFn().apply(evalContext);
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .putAllAsync(entries)
                .toCompletableFuture();
        await(future, timeout);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapUpdatePlan plan, List<Object> arguments, long timeout, @Nonnull SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = ExpressionEvalContext.createContext(
                args,
                hazelcastInstance,
                Util.getSerializationService(hazelcastInstance),
                ssc
        );

        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<Long> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, plan.updaterSupplier().get(evalContext))
                .toCompletableFuture();
        await(future, timeout);
        directIMapQueriesExecuted.getAndIncrement();
        return UpdateSqlResultImpl.createUpdateCountResult(0, plan.keyConditionParamIndex());
    }

    SqlResult execute(IMapDeletePlan plan, List<Object> arguments, long timeout, @Nonnull SqlSecurityContext ssc) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        ExpressionEvalContext evalContext = ExpressionEvalContext.createContext(
                args,
                hazelcastInstance,
                Util.getSerializationService(hazelcastInstance),
                ssc
        );

        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR)
                .toCompletableFuture();
        await(future, timeout);

        directIMapQueriesExecuted.getAndIncrement();

        return UpdateSqlResultImpl.createUpdateCountResult(0, plan.keyConditionParamIndex());
    }

    SqlResult execute(CreateTypePlan plan) {
        final Type type = new Type(plan.name(), plan.columns(), plan.options());
        catalog.createType(type, plan.replace(), plan.ifNotExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    // package-private for test purposes
    @Nonnull
    @SuppressWarnings("java:S3776")
    Set<Integer> tryUsePrunability(SelectPlan plan, ExpressionEvalContext evalContext) {
        Set<Integer> partitions = new HashSet<>();
        boolean allVariantsValid = true;
        for (final String mapName : plan.getPartitionStrategyCandidates().keySet()) {
            var perMapCandidates = plan.getPartitionStrategyCandidates().get(mapName);
            final PartitioningStrategy<?> strategy = ((MapProxyImpl) hazelcastInstance.getMap(mapName))
                    .getPartitionStrategy();

            // We only support Default and Attribute strategies, even if one of the maps uses non-Default/Attribute
            // strategy, we should abort the process and clear list of already populated partitions so that partition
            // pruning doesn't get activated at all for this query.
            if (strategy != null
                    && !(strategy instanceof DefaultPartitioningStrategy)
                    && !(strategy instanceof AttributePartitioningStrategy)) {
                allVariantsValid = false;
                break;
            }

            // ordering of attributes matters for partitioning (1,2) produces different partition than (2,1).
            final List<String> orderedKeyAttributes = new ArrayList<>();
            if (strategy instanceof AttributePartitioningStrategy) {
                final var attributeStrategy = (AttributePartitioningStrategy) strategy;
                orderedKeyAttributes.addAll(asList(attributeStrategy.getPartitioningAttributes()));
            } else {
                orderedKeyAttributes.add(KEY_ATTRIBUTE_NAME.value());
            }

            for (final Map<String, Expression<?>> perMapCandidate : perMapCandidates) {
                Object[] partitionKeyComponents = new Object[orderedKeyAttributes.size()];
                for (int i = 0; i < orderedKeyAttributes.size(); i++) {
                    final String attribute = orderedKeyAttributes.get(i);
                    if (!perMapCandidate.containsKey(attribute)) {
                        // Shouldn't happen, defensive check in case Opt logic breaks and produces variants
                        // that do not contain all the required partitioning attributes.
                        throw new HazelcastException("Partition Pruning candidate"
                                + " does not contain mandatory attribute: " + attribute);
                    }

                    partitionKeyComponents[i] = perMapCandidate.get(attribute).eval(null, evalContext);
                }

                final Integer partitionId = PartitioningStrategyUtil.getPartitionIdFromKeyComponents(
                        nodeEngine, strategy, partitionKeyComponents);

                if (partitionId == null) {
                    // The produced partitioning key is somehow invalid, most likely null.
                    // In this case we revert to non-pruning logic.
                    allVariantsValid = false;
                    break;
                }
                partitions.add(partitionId);
            }
        }
        return allVariantsValid && !partitions.isEmpty() ? partitions : emptySet();
    }

    // package-private for test purposes
    void registerJobInvocationObserver(SqlJobInvocationObserver jobInvocationObserver) {
        sqlJobInvocationObservers.add(jobInvocationObserver);
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

    private void broadcastUpdateDataConnectionOperations(@Nonnull String dataConnectionName) {
        List<Tuple2<Address, CompletableFuture<?>>> futures = new ArrayList<>();
        for (Member m : nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR)) {
            UpdateDataConnectionOperation op = new UpdateDataConnectionOperation(dataConnectionName);
            Address target = m.getAddress();
            InvocationFuture<Object> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, target)
                    .invoke();
            futures.add(tuple2(target, future));
        }

        for (Tuple2<Address, CompletableFuture<?>> tuple : futures) {
            try {
                assert tuple.f1() != null;
                tuple.f1().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                logger.warning("Failed to update data connection '" + dataConnectionName + "' on member '" + tuple.f0()
                        + "'. Background process should resolve this");
            }
        }
    }

    public long getDirectIMapQueriesExecuted() {
        return directIMapQueriesExecuted.get();
    }
}

package com.hazelcast.map.impl.query;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.operation.AbstractMapOperation;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class QueryOperationTestSupport {

    protected static final String MAP_NAME = "mapName";

    private final Answer<Data> randomDataAnswer = new DataAnswer();
    private final Collection<QueryableEntry> queryEntries = new ArrayList<QueryableEntry>();
    private final Set<QueryableEntry> queryEntrySet = new HashSet<QueryableEntry>();
    private final MapQueryEngine mapQueryEngine = mock(MapQueryEngine.class);
    private AbstractMapOperation queryOperation = createQueryOperation();

    protected abstract AbstractMapOperation createQueryOperation();

    /**
     * Initializes all mocks needed to execute a query operation.
     *
     * @param nodeResultLimit              configures the result size limit, use <code>Long.MAX_VALUE</code> to disable feature
     * @param numberOfReturnedQueryEntries configures the number of returned entries of the query operation
     */
    protected void initMocks(long nodeResultLimit, int numberOfReturnedQueryEntries) {
        for (int i = 0; i < numberOfReturnedQueryEntries; i++) {
            QueryableEntry queryableEntry = mock(QueryableEntry.class);
            when(queryableEntry.getKeyData()).thenAnswer(randomDataAnswer);
            when(queryableEntry.getIndexKey()).thenAnswer(randomDataAnswer);
            when(queryableEntry.getValueData()).thenAnswer(randomDataAnswer);

            queryEntries.add(queryableEntry);
            queryEntrySet.add(queryableEntry);
        }

        QueryResult queryResult = new QueryResult(nodeResultLimit);

        when(mapQueryEngine.newQueryResult(anyInt())).thenReturn(queryResult);
        when(mapQueryEngine.queryOnPartition(MAP_NAME, TruePredicate.INSTANCE, Operation.GENERIC_PARTITION_ID))
                .thenReturn(queryEntries);

        IndexService indexService = mock(IndexService.class);
        when(indexService.query(TruePredicate.INSTANCE)).thenReturn(queryEntrySet);

        MapConfig mapConfig = mock(MapConfig.class);
        when(mapConfig.isStatisticsEnabled()).thenReturn(false);

        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        when(mapServiceContext.getMapQueryEngine()).thenReturn(mapQueryEngine);

        MapService mapService = mock(MapService.class);
        when(mapService.getMapServiceContext()).thenReturn(mapServiceContext);

        queryOperation.setMapService(mapService);

        MapContainer mapContainer = mock(MapContainer.class);
        when(mapContainer.getIndexService()).thenReturn(indexService);
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);

        queryOperation.setMapContainer(mapContainer);

        InternalPartitionService partitionService = mock(InternalPartitionService.class);
        when(partitionService.getPartitionStateVersion()).thenReturn(0);
        when(partitionService.hasOnGoingMigrationLocal()).thenReturn(false);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getPartitionService()).thenReturn(partitionService);

        queryOperation.setNodeEngine(nodeEngine);
    }

    /**
     * Executes the query operation and returns the {@link QueryResult}.
     *
     * @return {@link QueryResult}
     */
    protected QueryResult getQueryResult() throws Exception {
        queryOperation.run();
        return (QueryResult) queryOperation.getResponse();
    }

    private static class DataAnswer implements Answer<Data> {

        private static final Random random = new Random();

        @Override
        public Data answer(InvocationOnMock invocation) throws Throwable {
            byte[] randomBytes = new byte[20];
            random.nextBytes(randomBytes);

            return new HeapData(randomBytes);
        }
    }
}

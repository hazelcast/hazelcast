package com.hazelcast.map.impl;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.MapLoader;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.map.impl.operation.PartitionCheckIfLoadedOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.UnmodifiableIterator;
import com.hazelcast.util.ValidationUtil;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.eviction.MaxSizeChecker.getApproximateMaxSize;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.spi.ExecutionService.MAP_LOAD_ALL_KEYS_EXECUTOR;
import static com.hazelcast.util.IterableUtil.limit;
import static com.hazelcast.util.IterableUtil.map;

/**
 * Loads keys from a {@link MapLoader} and sends them to all partitions for loading
 */
public class MapKeyLoader {

    private String mapName;
    private OperationService opService;
    private InternalPartitionService partitionService;
    private IFunction<Object, Data> toData;
    private ExecutionService execService;

    private int maxSize;
    private int maxBatch;
    private int mapNamePartition;
    private boolean loadInitiated;

    private LoadFinishedFuture loadFinished = new LoadFinishedFuture(true);

    public MapKeyLoader(String mapName, OperationService opService, InternalPartitionService ps,
            ExecutionService execService, IFunction<Object, Data> serialize) {
        this.mapName = mapName;
        this.opService = opService;
        this.partitionService = ps;
        this.toData = serialize;
        this.execService = execService;
    }

    /**
     * Sends keys to all partitions in batches.
     */
    public Future<?> sendKeys(final MapStoreContext mapStoreContext, final boolean replaceExistingValues) {

        if (loadFinished.isDone()) {

            loadFinished = new LoadFinishedFuture();
            loadInitiated = true;

            Future<Boolean> sent = execService.submit(MAP_LOAD_ALL_KEYS_EXECUTOR, new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Iterable<Object> allKeys = mapStoreContext.loadAllKeys();
                    sendKeysInBatches(allKeys, replaceExistingValues);
                    return false;
                }
            });

            execService.asCompletableFuture(sent).andThen(loadFinished);
        }

        return loadFinished;
    }

    /**
     * Check if loaded on loader partition. Triggers key loading if it hadn't started
     */
    public Future triggerLoading() {

        if (loadFinished.isDone()) {

            loadFinished = new LoadFinishedFuture();

            execService.execute(MAP_LOAD_ALL_KEYS_EXECUTOR, new Runnable() {
                @Override
                public void run() {
                    Operation op = new PartitionCheckIfLoadedOperation(mapName, true);
                    opService.<Boolean>invokeOnPartition(SERVICE_NAME, op, mapNamePartition).andThen(loadFinished);
                }
            });
        }

        return loadFinished;
    }

    public void completeLoading() {
        if (!loadFinished.isDone()) {
            loadFinished.setResult(true);
        }
    }

    public Future startInitialLoad(MapStoreContext mapStoreContext, int partitionId) {

        this.mapNamePartition = partitionService.getPartitionId(toData.apply(mapName));

        if (!partitionService.isPartitionOwner(partitionId)) {
            return loadFinished;
        }

        if (partitionId == mapNamePartition) {
            return sendKeys(mapStoreContext, false);
        } else {
            return triggerLoading();
        }
    }

    private void sendKeysInBatches(Iterable<Object> allKeys, boolean replaceExistingValues) {

        Iterator<Object> keys = allKeys.iterator();
        Iterator<Data> dataKeys = map(keys, toData);

        if (maxSize > 0) {
            dataKeys = limit(dataKeys, maxSize);
        }

        Iterator<Entry<Integer, Data>> partitionsAndKeys = map(dataKeys, toPartition());
        Iterator<Map<Integer, List<Data>>> batches = toBatches(partitionsAndKeys, maxBatch);

        while (batches.hasNext()) {
            Map<Integer, List<Data>> batch = batches.next();
            sendBatch(batch, replaceExistingValues);
        }

        sendLoadCompleted(partitionService.getPartitionCount(), replaceExistingValues);

        if (keys instanceof Closeable) {
            closeResource((Closeable) keys);
        }
    }

    public static Iterator<Map<Integer, List<Data>>> toBatches(final Iterator<Entry<Integer, Data>> entries,
            final int maxBatch) {

        return new UnmodifiableIterator<Map<Integer, List<Data>>>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public Map<Integer, List<Data>> next() {
                ValidationUtil.checkHasNext(entries, "No next element");
                return nextBatch(entries, maxBatch);
            }
        };
    }

    private IFunction<Data, Entry<Integer, Data>> toPartition() {
        return new IFunction<Data, Entry<Integer, Data>>() {
            @Override
            public Entry<Integer, Data> apply(Data input) {
                Integer partition = partitionService.getPartitionId(input);
                return new MapEntrySimple<Integer, Data>(partition, input);
            }
        };
    }

    private static Map<Integer, List<Data>> nextBatch(Iterator<Entry<Integer, Data>> entries, int maxBatch) {

        Map<Integer, List<Data>> batch = new HashMap<Integer, List<Data>>();

        while (entries.hasNext()) {
            Entry<Integer, Data> e = entries.next();
            List<Data> partitionKeys = CollectionUtil.addToValueList(batch, e.getKey(), e.getValue());

            if (partitionKeys.size() >= maxBatch) {
                break;
            }
        }

        return batch;
    }

    private void sendBatch(Map<Integer, List<Data>> batch, boolean replaceExistingValues) {

        for (Entry<Integer, List<Data>> e : batch.entrySet()) {
            int partitionId = e.getKey();
            List<Data> keys = e.getValue();
            LoadAllOperation op = new LoadAllOperation(mapName, keys, replaceExistingValues, false);
            opService.invokeOnPartition(SERVICE_NAME, op, partitionId);
        }

    }

    private List<Future<Object>> sendLoadCompleted(int partitions, boolean replaceExistingValues) {

        List<Future<Object>> futures = new ArrayList<Future<Object>>();
        boolean lastBatch = true;

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            LoadAllOperation op = new LoadAllOperation(mapName, Collections.<Data>emptyList(), replaceExistingValues, lastBatch);
            futures.add(opService.invokeOnPartition(SERVICE_NAME, op, partitionId));
        }

        return futures;
    }

    public static int getMaxSize(int clusterSize, MaxSizeConfig maxSizeConfig) {
        int maxSizePerNode = getApproximateMaxSize(maxSizeConfig, MaxSizePolicy.PER_NODE);
        if (maxSizePerNode == MaxSizeConfig.DEFAULT_MAX_SIZE) {
            // unlimited
            return -1;
        }
        int maxSize = clusterSize * maxSizePerNode;
        return maxSize;
    }

    public boolean isLoadInitiated() {
        return loadInitiated;
    }

    public void setMaxBatch(int maxBatch) {
        this.maxBatch = maxBatch;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    private static final class LoadFinishedFuture extends AbstractCompletableFuture<Boolean>
        implements ExecutionCallback<Boolean> {

        private LoadFinishedFuture() {
            super(null, null);
        }

        private LoadFinishedFuture(Boolean result) {
            super(null, null);
            setResult(result);
        }

        @Override
        public Boolean get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
            if (isDone()) {
                return getResult();
            }
            throw new UnsupportedOperationException("Future is not done yet");
        }

        @Override
        public void onResponse(Boolean loaded) {
            if (loaded) {
                setResult(loaded);
            }
            // if not loaded yet we wait for the last batch to arrive
        }

        @Override
        public void onFailure(Throwable t) {
            setResult(t);
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{done=" + isDone() + "}";
        }
    }
}

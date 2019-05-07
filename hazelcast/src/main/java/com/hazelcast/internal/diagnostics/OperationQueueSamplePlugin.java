package com.hazelcast.internal.diagnostics;

import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.spi.serialization.SerializationService;

import java.text.NumberFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OperationQueueSamplePlugin extends DiagnosticsPlugin {

    /**
     * The sample period in seconds.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty SAMPLE_PERIOD_SECONDS
            = new HazelcastProperty(PREFIX + ".operationqueue.period.seconds", 0, SECONDS);

    private final NumberFormat defaultFormat = NumberFormat.getPercentInstance();

    private final OperationExecutorImpl operationExecutor;
    private final long samplePeriodMillis;
    private final SerializationService serializationService;

    public OperationQueueSamplePlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(PendingInvocationsPlugin.class));
        InternalOperationService operationService = nodeEngine.getOperationService();
        this.operationExecutor = (OperationExecutorImpl) ((OperationServiceImpl) operationService).getOperationExecutor();
        HazelcastProperties props = nodeEngine.getProperties();
        this.samplePeriodMillis = props.getMillis(SAMPLE_PERIOD_SECONDS);
        serializationService = nodeEngine.getSerializationService();
    }

    @Override
    public long getPeriodMillis() {
        return samplePeriodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: period-millis:" + samplePeriodMillis);
    }

    @Override
    public void onStop() {
        executor.shutdown();
    }

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Override
    public void run(DiagnosticsLogWriter writer) {
        writer.startSection("OperationQueueSamples");

        final ConcurrentMap<Class, AtomicLong> map = new ConcurrentHashMap();
        final CountDownLatch latch = new CountDownLatch(operationExecutor.getPartitionThreadCount());
        for (int k = 0; k < operationExecutor.getPartitionThreadCount(); k++) {
            final PartitionOperationThread operationThread = operationExecutor.partitionThreads[k];
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    operationThread.queue.sample(map, serializationService);
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
        }
        render(writer, map);
        writer.endSection();
    }


    private void render(DiagnosticsLogWriter writer, ConcurrentMap<Class, AtomicLong> occurrenceMap) {
        long sampleCount = 0;
        for (Class key : occurrenceMap.keySet()) {
            AtomicLong value = occurrenceMap.get(key);
            sampleCount += value.get();
        }


        for (Class key : occurrenceMap.keySet()) {
            AtomicLong value = occurrenceMap.get(key);
            if (value.get() == 0) {
                continue;
            }

            double percentage = (1d * value.get()) / sampleCount;
            writer.writeEntry(key + " sampleCount=" + value + " " + defaultFormat.format(percentage));
        }

    }

}

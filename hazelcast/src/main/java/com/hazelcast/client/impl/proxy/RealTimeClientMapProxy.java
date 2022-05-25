package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.logging.ILogger;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RealTimeClientMapProxy extends ClientMapProxy {
    public final static String LIMIT_NAME = "limit";
    public final static String PUT_OPERATION_NAME = "put";
    public final static String GET_OPERATION_NAME = "get";

    private ILogger logger;
    private long limitMsecs;
    public String limitString;

    private final ConcurrentHashMap<String, AtomicLong> realTimeStats = new ConcurrentHashMap();

    public RealTimeClientMapProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);

        logger = context.getLoggingService().getLogger(RealTimeClientMapProxy.class);

        Long limit = context.getClientConfig().getRealTimeConfig().getMapLimit(name);
        if (limit != null) {
            limitMsecs = limit;
            limitString = Long.toString(limit);
        }
    }

    public ConcurrentHashMap<String, AtomicLong> getRealTimeStats() {
        return realTimeStats;
    }

    public long getPutLatency() {
        return 0;
    }

    public String getLimitString() {
        return limitString;
    }

    @Override
    public Object get(@NotNull Object key) {
        long start = System.nanoTime();
        Object value = super.get(key);

        updateLatency(start, GET_OPERATION_NAME);

        return value;
    }

    @Override
    public Object put(@NotNull Object key, @NotNull Object value) {
        long start = System.nanoTime();
        Object oldValue = super.put(key, value);

        updateLatency(start, PUT_OPERATION_NAME);

        return oldValue;
    }

    private void updateLatency(long start, String putOperationName) {
        long endTime = System.nanoTime();
        long latency = endTime - start;

        if (TimeUnit.MILLISECONDS.convert(latency, TimeUnit.NANOSECONDS) > limitMsecs) {
            logger.warning("The real-time configured limit (" + limitMsecs + " msecs) is exceeded for "
                    + PUT_OPERATION_NAME + " for map " + getName() + ". The measured latency is " + latency + " msecs.");
        }

        realTimeStats.compute(putOperationName, (opName, currentLatency) -> {
            if (currentLatency == null) {
                currentLatency = new AtomicLong();
            }
            currentLatency.getAndUpdate(v -> Math.max(latency, v));
            return currentLatency;
        });
    }
}

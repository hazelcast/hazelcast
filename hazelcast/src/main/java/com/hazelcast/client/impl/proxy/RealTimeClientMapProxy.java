/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RealTimeClientMapProxy extends ClientMapProxy {
    public static final String LIMIT_NAME = "limit";
    public static final String PUT_OPERATION_NAME = "put";
    public static final String GET_OPERATION_NAME = "get";

    private ILogger logger;
    private long limitNsecs;
    private String limitString;

    private final ConcurrentHashMap<String, AtomicLong> realTimeStats = new ConcurrentHashMap();

    public RealTimeClientMapProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);

        logger = context.getLoggingService().getLogger(RealTimeClientMapProxy.class);

        Long limit = context.getClientConfig().getRealTimeConfig().getMapLimit(name);
        if (limit != null) {
            limitNsecs = limit;
            limitString = Long.toString(limit);
        }
    }

    public ConcurrentHashMap<String, AtomicLong> getRealTimeStats() {
        return realTimeStats;
    }

    public void setLimitString(String limitString) {
        this.limitString = limitString;
    }

    public long getPutLatencyAndReset() {
        return getLatencyAndReset(PUT_OPERATION_NAME);
    }

    public long getGetLatencyAndReset() {
        return getLatencyAndReset(GET_OPERATION_NAME);
    }

    private long getLatencyAndReset(String opName) {
        AtomicLong latency = realTimeStats.get(opName);
        if (latency != null) {
            return latency.getAndSet(0);
        }

        return 0;
    }

    public String getLimitString() {
        return limitString;
    }

    @Override
    public Object get(@Nonnull Object key) {
        long start = System.nanoTime();
        Object value = super.get(key);

        updateLatency(start, GET_OPERATION_NAME);

        return value;
    }

    @Override
    public Object put(@Nonnull Object key, @Nonnull Object value) {
        long start = System.nanoTime();
        Object oldValue = super.put(key, value);

        updateLatency(start, PUT_OPERATION_NAME);

        return oldValue;
    }

    private void updateLatency(long start, String operationName) {
        long endTime = System.nanoTime();
        long latency = endTime - start;

        if (latency > limitNsecs) {
            logger.warning("The real-time configured limit (" + limitNsecs + " nsecs) is exceeded for "
                    + operationName + " for map " + getName() + ". The measured latency is " + latency + " nsecs.");
        } else {
            if (logger.isFineEnabled()) {
                logger.fine("The real-time configured limit (" + limitNsecs + " nsecs) for "
                        + operationName + " for map " + getName() + ". The measured latency is " + latency + " nsecs.");
            }
        }

        realTimeStats.compute(operationName, (opName, currentLatency) -> {
            if (currentLatency == null) {
                currentLatency = new AtomicLong();
            }
            currentLatency.getAndUpdate(v -> Math.max(latency, v));
            return currentLatency;
        });
    }
}

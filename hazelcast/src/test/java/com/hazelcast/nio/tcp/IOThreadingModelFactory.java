package com.hazelcast.nio.tcp;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.IOThreadingModel;

public interface IOThreadingModelFactory {
    IOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry);
}

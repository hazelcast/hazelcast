package com.hazelcast.nio.tcp;

import com.hazelcast.internal.metrics.MetricsRegistry;

public interface IOThreadingModelFactory {
    IOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry);
}

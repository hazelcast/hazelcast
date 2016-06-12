package com.hazelcast.internal.connection.tcp;

import com.hazelcast.internal.metrics.MetricsRegistry;

public interface IOThreadingModelFactory {
    IOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry);
}

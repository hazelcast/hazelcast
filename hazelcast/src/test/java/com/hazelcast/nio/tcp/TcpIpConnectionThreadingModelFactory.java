package com.hazelcast.nio.tcp;

import com.hazelcast.internal.metrics.MetricsRegistry;

public interface TcpIpConnectionThreadingModelFactory {
    TcpIpConnectionThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry);
}

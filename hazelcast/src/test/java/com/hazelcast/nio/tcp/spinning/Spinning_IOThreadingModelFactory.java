package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;

public class Spinning_IOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public SpinningIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {
        return new SpinningIOThreadingModel(
                ioService,
                ioService.loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup);
    }
}

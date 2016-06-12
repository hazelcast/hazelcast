package com.hazelcast.internal.connection.tcp.spinning;

import com.hazelcast.internal.connection.tcp.IOThreadingModelFactory;
import com.hazelcast.internal.connection.tcp.MockIOService;
import com.hazelcast.internal.metrics.MetricsRegistry;

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

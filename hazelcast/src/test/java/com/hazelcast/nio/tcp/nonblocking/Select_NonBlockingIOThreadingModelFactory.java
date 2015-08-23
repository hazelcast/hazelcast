package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;

public class Select_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(
            MockIOService ioService, MetricsRegistry metricsRegistry) {
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                ioService,
                ioService.loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup);
        threadingModel.setInputSelectNow(false);
        threadingModel.setOutputSelectNow(false);
        return threadingModel;
    }
}

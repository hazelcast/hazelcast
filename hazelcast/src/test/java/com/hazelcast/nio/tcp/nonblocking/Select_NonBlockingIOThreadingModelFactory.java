package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;

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

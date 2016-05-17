package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;

public class SelectNow_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(
            MockIOService ioService, MetricsRegistry metricsRegistry) {
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                ioService,
                ioService.loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup);
        threadingModel.setSelectorMode(SelectorMode.SELECT_NOW);
        return threadingModel;
    }
}

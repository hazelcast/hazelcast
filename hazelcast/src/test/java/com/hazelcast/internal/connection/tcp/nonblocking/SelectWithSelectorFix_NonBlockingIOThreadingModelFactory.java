package com.hazelcast.internal.connection.tcp.nonblocking;

import com.hazelcast.internal.connection.tcp.IOThreadingModelFactory;
import com.hazelcast.internal.connection.tcp.MockIOService;
import com.hazelcast.internal.metrics.MetricsRegistry;

public class SelectWithSelectorFix_NonBlockingIOThreadingModelFactory
        implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(
            MockIOService ioService, MetricsRegistry metricsRegistry) {
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                ioService,
                ioService.loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup);
        threadingModel.setSelectorMode(SelectorMode.SELECT_WITH_FIX);
        threadingModel.setSelectorWorkaroundTest(true);
        return threadingModel;
    }
}

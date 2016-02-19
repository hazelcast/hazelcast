package com.hazelcast.internal.connection.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.connection.tcp.MockIOService;
import com.hazelcast.internal.connection.tcp.IOThreadingModelFactory;

public class SelectNow_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(
            MockIOService ioService, MetricsRegistry metricsRegistry) {
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                ioService,
                ioService.loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup);
        threadingModel.setInputSelectNow(true);
        threadingModel.setOutputSelectNow(true);
        return threadingModel;
    }
}

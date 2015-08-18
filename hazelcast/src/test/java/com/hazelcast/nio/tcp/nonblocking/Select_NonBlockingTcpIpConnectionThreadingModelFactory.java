package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.TcpIpConnectionThreadingModelFactory;

public class Select_NonBlockingTcpIpConnectionThreadingModelFactory implements TcpIpConnectionThreadingModelFactory {

    @Override
    public NonBlockingTcpIpConnectionThreadingModel create(
            MockIOService ioService, MetricsRegistry metricsRegistry) {
        NonBlockingTcpIpConnectionThreadingModel threadingModel = new NonBlockingTcpIpConnectionThreadingModel(
                ioService,
                ioService.loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup);
        threadingModel.setInputSelectNow(false);
        threadingModel.setOutputSelectNow(false);
        return threadingModel;
    }
}

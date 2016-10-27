package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.SocketReaderInitializerImpl;
import com.hazelcast.nio.tcp.SocketWriterInitializerImpl;

public class SelectNow_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {

        LoggingServiceImpl loggingService = ioService.loggingService;
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup,
                ioService.getIoOutOfMemoryHandler(), ioService.getInputSelectorThreadCount(),
                ioService.getOutputSelectorThreadCount(),
                ioService.getBalancerIntervalSeconds(),
                new SocketWriterInitializerImpl(loggingService.getLogger(SocketWriterInitializerImpl.class)),
                new SocketReaderInitializerImpl(loggingService.getLogger(SocketReaderInitializerImpl.class))
        );
        threadingModel.setSelectorMode(SelectorMode.SELECT_NOW);
        return threadingModel;
    }
}

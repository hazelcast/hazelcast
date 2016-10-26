package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.internal.networking.nonblocking.SelectorMode;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.SocketReaderInitializerImpl;
import com.hazelcast.nio.tcp.SocketWriterInitializerImpl;

public class Select_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {
        LoggingService loggingService = ioService.loggingService;
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
        threadingModel.setSelectorMode(SelectorMode.SELECT);
        return threadingModel;
    }
}

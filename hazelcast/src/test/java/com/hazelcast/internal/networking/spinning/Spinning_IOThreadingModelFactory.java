package com.hazelcast.internal.networking.spinning;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.SocketReaderInitializerImpl;
import com.hazelcast.nio.tcp.SocketWriterInitializerImpl;

public class Spinning_IOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public SpinningIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {
        LoggingService loggingService = ioService.loggingService;
        return new SpinningIOThreadingModel(
                loggingService,
                ioService.hazelcastThreadGroup,
                ioService.getIoOutOfMemoryHandler(),
                new SocketWriterInitializerImpl(loggingService.getLogger(SocketWriterInitializerImpl.class)),
                new SocketReaderInitializerImpl(loggingService.getLogger(SocketReaderInitializerImpl.class)));
    }
}

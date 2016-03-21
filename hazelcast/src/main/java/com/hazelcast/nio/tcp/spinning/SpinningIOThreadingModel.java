/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.IOThreadingModel;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.nio.tcp.TcpIpConnection;

/**
 * A {@link IOThreadingModel} that uses (busy) spinning on the SocketChannels to see if there is something
 * to read or write.
 *
 * Currently there are 2 threads spinning:
 * <ol>
 * <li>1 thread spinning on all SocketChannels for reading</li>
 * <li>1 thread spinning on all SocketChannels for writing</li>
 * </ol>
 * In the future we need to play with this a lot more. 1 thread should be able to saturate a 40GbE connection, but that
 * currently doesn't work for us. So I guess our IO threads are doing too much stuff not relevant like writing the Frames
 * to bytebuffers or converting the bytebuffers to Frames.
 *
 * This is an experimental feature and disabled by default.
 */
public class SpinningIOThreadingModel implements IOThreadingModel {

    private final ILogger logger;
    private final MetricsRegistry metricsRegistry;
    private final LoggingService loggingService;
    private final SpinningInputThread inputThread;
    private final SpinningOutputThread outThread;

    public SpinningIOThreadingModel(
            IOService ioService,
            LoggingService loggingService,
            MetricsRegistry metricsRegistry,
            HazelcastThreadGroup hazelcastThreadGroup) {
        this.logger = loggingService.getLogger(SpinningIOThreadingModel.class);
        this.metricsRegistry = metricsRegistry;
        this.loggingService = loggingService;
        this.inputThread = new SpinningInputThread(hazelcastThreadGroup, loggingService.getLogger(SpinningInputThread.class));
        this.outThread = new SpinningOutputThread(hazelcastThreadGroup, loggingService.getLogger(SpinningOutputThread.class));
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    @Override
    public SocketWriter newSocketWriter(TcpIpConnection connection) {
        ILogger logger = loggingService.getLogger(SpinningSocketWriter.class);
        return new SpinningSocketWriter(connection, metricsRegistry, logger);
    }

    @Override
    public SocketReader newSocketReader(TcpIpConnection connection) {
        ILogger logger = loggingService.getLogger(SpinningSocketReader.class);
        return new SpinningSocketReader(connection, metricsRegistry, logger);
    }

    @Override
    public void onConnectionAdded(TcpIpConnection connection) {
        inputThread.addConnection(connection);
        outThread.addConnection(connection);
    }

    @Override
    public void onConnectionRemoved(TcpIpConnection connection) {
        inputThread.removeConnection(connection);
        outThread.removeConnection(connection);
    }

    @Override
    public void start() {
        logger.info("TcpIpConnectionManager configured with Spinning IO-threading model: "
                + "1 input thread and 1 output thread");
        inputThread.start();
        outThread.start();
    }

    @Override
    public void shutdown() {
        inputThread.shutdown();
        outThread.shutdown();
    }
}

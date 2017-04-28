/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.spinning;

import com.hazelcast.internal.networking.IOOutOfMemoryHandler;
import com.hazelcast.internal.networking.IOThreadingModel;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.SocketReader;
import com.hazelcast.internal.networking.SocketReaderInitializer;
import com.hazelcast.internal.networking.SocketWriter;
import com.hazelcast.internal.networking.SocketWriterInitializer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

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
    private final LoggingService loggingService;
    private final SpinningInputThread inputThread;
    private final SpinningOutputThread outThread;
    private final SocketWriterInitializer socketWriterInitializer;
    private final SocketReaderInitializer socketReaderInitializer;
    private final IOOutOfMemoryHandler oomeHandler;

    public SpinningIOThreadingModel(LoggingService loggingService,
                                    IOOutOfMemoryHandler oomeHandler,
                                    SocketWriterInitializer socketWriterInitializer,
                                    SocketReaderInitializer socketReaderInitializer,
                                    String hzName) {
        this.logger = loggingService.getLogger(SpinningIOThreadingModel.class);
        this.loggingService = loggingService;
        this.oomeHandler = oomeHandler;
        this.inputThread = new SpinningInputThread(hzName);
        this.outThread = new SpinningOutputThread(hzName);
        this.socketWriterInitializer = socketWriterInitializer;
        this.socketReaderInitializer = socketReaderInitializer;
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    @Override
    public SocketWriter newSocketWriter(SocketConnection connection) {
        ILogger logger = loggingService.getLogger(SpinningSocketWriter.class);
        return new SpinningSocketWriter(connection, logger, oomeHandler, socketWriterInitializer);
    }

    @Override
    public SocketReader newSocketReader(SocketConnection connection) {
        ILogger logger = loggingService.getLogger(SpinningSocketReader.class);
        return new SpinningSocketReader(connection, logger, oomeHandler, socketReaderInitializer);
    }

    @Override
    public void onConnectionAdded(SocketConnection connection) {
        inputThread.addConnection(connection);
        outThread.addConnection(connection);
    }

    @Override
    public void onConnectionRemoved(SocketConnection connection) {
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

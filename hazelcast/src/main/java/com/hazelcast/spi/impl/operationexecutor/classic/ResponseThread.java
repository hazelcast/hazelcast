/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;

/**
 * The ResponseThread is responsible for processing responses-packets.
 *
 * So when a response is received from a remote system, it is put in the workQueue of the ResponseThread.
 * Then the ResponseThread takes it from this workQueue and calls the {@link ResponsePacketHandler} for the
 * actual processing.
 *
 * The reason that the IO thread doesn't immediately deals with the response is that deserializing the
 * {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} and let the invocation-future
 * deal with the response can be rather expensive currently.
 *
 * This class needs to implement the OperationHostileThread interface to make sure that the OperationExecutor
 * is not going to schedule any operations on this task due to retry.
 */
public final class ResponseThread extends Thread implements OperationHostileThread {

    final BlockingQueue<Packet> workQueue = new LinkedBlockingQueue<Packet>();
    // field is only written by the response-thread itself, but can be read by other threads.
    volatile long processedResponses;

    private final ILogger logger;
    private final ResponsePacketHandler responsePacketHandler;
    private volatile boolean shutdown;

    public ResponseThread(HazelcastThreadGroup threadGroup, ILogger logger,
                          ResponsePacketHandler responsePacketHandler) {
        super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("response"));
        setContextClassLoader(threadGroup.getClassLoader());
        this.logger = logger;
        this.responsePacketHandler = responsePacketHandler;
    }

    @Override
    public void run() {
        try {
            doRun();
        } catch (Throwable t) {
            inspectOutputMemoryError(t);
            logger.severe(t);
        }
    }

    private void doRun() {
        for (; ; ) {
            Packet responsePacket;
            try {
                responsePacket = workQueue.take();
            } catch (InterruptedException e) {
                if (shutdown) {
                    return;
                }
                continue;
            }

            if (shutdown) {
                return;
            }

            process(responsePacket);
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({ "VO_VOLATILE_INCREMENT" })
    private void process(Packet responsePacket) {
        processedResponses++;
        try {
            responsePacketHandler.handle(responsePacket);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process response: " + responsePacket + " on response thread:" + getName());
        }
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }
}

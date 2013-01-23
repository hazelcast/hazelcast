///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.client;
//
//import com.hazelcast.nio.Protocol;
//import com.hazelcast.nio.protocol.Command;
//import com.hazelcast.util.Clock;
//
//import java.io.IOException;
//import java.util.Map;
//import java.util.logging.Level;
//
//public class InRunnable extends IORunnable implements Runnable {
//    final ProtocolReader reader;
//    Connection connection = null;
//    private final OutRunnable outRunnable;
//
//    volatile long lastReceived;
//
//    public InRunnable(HazelcastClient client, OutRunnable outRunnable, Map<Long, Call> calls, ProtocolReader reader) {
//        super(client, calls);
//        this.outRunnable = outRunnable;
//        this.reader = reader;
//    }
//
//    protected void customRun() throws InterruptedException {
//        if (outRunnable.reconnection.get()) {
//            Thread.sleep(10L);
//            return;
//        }
//        Protocol protocol;
//        try {
//            Connection oldConnection = connection;
//            connection = client.getConnectionManager().getConnection();
//            if (restoredConnection(oldConnection, connection)) {
//                if (outRunnable.sendReconnectCall(connection)) {
//                    logger.log(Level.FINEST, "restoredConnection");
//                    if (oldConnection != null) {
//                        redoUnfinishedCalls(oldConnection);
//                    }
//                }
//                return;
//            }
//            if (connection == null) {
//                outRunnable.clusterIsDown(oldConnection);
//                Thread.sleep(10);
//            } else {
//                protocol = reader.read(connection);
////                logger.log(Level.FINEST, "Reading " + packet.getOperation() + " Call id: " + packet.getCallId());
//                this.lastReceived = Clock.currentTimeMillis();
//                long callId = protocol.flag == null ? -1 : Long.valueOf(protocol.flag);
//                Call call = callMap.remove(callId);
//                if (call != null) {
//                    call.received = System.nanoTime();
//                    call.setResponse(protocol);
//                } else {
//                    if (protocol.command.equals(Command.EVENT) ||
//                            protocol.command.equals(Command.QEVENT) ||
//                            protocol.command.equals(Command.MESSAGE)) {
//                        client.getListenerManager().enqueue(protocol);
//                    }
//                    if (callId != -1) {
//                        logger.log(Level.SEVERE, "In Thread can not handle: " + protocol.command + " : " + protocol.flag);
//                    }
//                }
//            }
//        } catch (Throwable e) {
//            e.printStackTrace();
//            logger.log(Level.FINEST, "InRunnable [" + connection + "] got an exception:" + e.toString(), e);
//            outRunnable.clusterIsDown(connection);
//        }
//    }
//
//    private void redoUnfinishedCalls(Connection oldConnection) {
//        onDisconnect(oldConnection);
//    }
//
//    public void shutdown() {
//        synchronized (monitor) {
//            if (running) {
//                this.running = false;
//                try {
//                    Connection connection = client.getConnectionManager().getConnection();
//                    if (connection != null) {
//                        connection.close();
//                    }
//                } catch (IOException ignored) {
//                }
//                try {
//                    monitor.wait(5000L);
//                } catch (InterruptedException ignored) {
//                }
//            }
//        }
//    }
//}

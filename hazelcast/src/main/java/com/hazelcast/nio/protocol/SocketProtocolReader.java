/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio.protocol;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.nio.ascii.SocketTextReader;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;
//    COMMAND <arg1>É<argN> \r\n
//    #M  <s1>É<sM> \r\n
//    B1B2É. BM

public class SocketProtocolReader implements SocketReader {

    final ILogger logger;

    ByteBuffer commandLine = ByteBuffer.allocate(500);
    MutableBoolean commandLineRead = new MutableBoolean(false);

    ByteBuffer sizeLine = ByteBuffer.allocate(500);
    MutableBoolean sizeLineRead = new MutableBoolean(false);

    String command;
    String[] args;
    ByteBuffer[] buffers;
    int[] bufferSize;

    private final Connection connection;
    private final IOService ioService;

    public SocketProtocolReader(SocketChannelWrapper socketChannel, Connection connection) {
        this.connection = connection;
        this.ioService = connection.getConnectionManager().getIOHandler();
        this.logger = ioService.getLogger(SocketProtocolReader.class.getName());
    }

    public void read(ByteBuffer bb) throws Exception {
        while (bb.hasRemaining()) {
            doRead(bb);
        }
    }

    private void doRead(ByteBuffer bb) {
        try {
            readUntilTheEndOfLine(bb, commandLineRead, commandLine);
            if (commandLineRead.get()) {
                parseCommandLine(SocketTextReader.toStringAndClear(commandLine));
                readUntilTheEndOfLine(bb, sizeLineRead, sizeLine);
                if (sizeLineRead.get()) {
                    parseSizeLine(SocketTextReader.toStringAndClear(sizeLine));
                    for (int i = 0; buffers != null && bb.hasRemaining() && i < buffers.length; i++) {
                        if (buffers[i].hasRemaining()) {
                            copy(bb, buffers[i]);
                        }
                    }
                    if (buffers != null && (buffers.length == 0 || !buffers[buffers.length - 1].hasRemaining())) {
                        Protocol protocol = new Protocol(connection, command, args, buffers);
                        connection.setType(Connection.Type.PROTOCOL_CLIENT);
                        ioService.handleClientCommand(protocol);
                        reset();
                    }
                }
            }
        } catch (Exception e) {
            connection.getWriteHandler().enqueueSocketWritable(new Protocol(connection, 
                    Command.ERROR.value, new String[]{"0", "Malformed_request", e.toString()}));
            logger.log(Level.SEVERE, e.toString());
        }
    }

    void copy(ByteBuffer from, ByteBuffer to) {
        if (from.isDirect()) {
            int n = Math.min(from.remaining(), to.remaining());
            if (n > 0) {
                from.get(to.array(), to.position(), n);
                to.position(to.position() + n);
            }
        } else {
            IOUtil.copyToHeapBuffer(from, to);
        }
    }

    private void reset() {
        commandLine.clear();
        commandLineRead.set(false);
        sizeLine.clear();
        sizeLineRead.set(false);
        command = null;
        args = null;
        buffers = null;
        bufferSize = null;
    }

    private void parseSizeLine(String line) {
        if (buffers != null) return;
        System.out.println("Size line is : " + line);
        String[] split = line.split("\\s");
        if (!split[0].startsWith("#")) {
            throw new RuntimeException("Invalid Command, the size line should start with #");
        }
        int bufferCount = Integer.parseInt(split[0].substring(1));
        buffers = new ByteBuffer[bufferCount];
        bufferSize = new int[bufferCount];
        for (int i = 0; i < bufferCount; i++) {
            bufferSize[i] = Integer.parseInt(split[i+1]);
        }
        for (int i = 0; i < bufferCount; i++) {
            buffers[i] = ByteBuffer.allocate(bufferSize[i]);
        }
    }

    private void parseCommandLine(String line) {
        if (sizeLineRead.get()) return;
        System.out.println("Command line is : " + line);
        String[] split = line.split("\\s");
        command = split[0];
        args = new String[split.length - 1];
        for (int i = 1; i < split.length; i++) {
            args[i - 1] = split[i];
        }
    }

    private void readUntilTheEndOfLine(ByteBuffer bb, MutableBoolean lineIsRead, ByteBuffer line) {
        while (!lineIsRead.get() && bb.hasRemaining()) {
            byte b = bb.get();
            char c = (char) b;
            if (c == '\n') {
                lineIsRead.set(true);
            } else if (c != '\r') {
                line.put(b);
            }
        }
    }

    public static class MutableBoolean {
        Boolean value;

        public MutableBoolean(Boolean value) {
            this.value = value;
        }

        public Boolean get() {
            return value;
        }

        public void set(Boolean value) {
            this.value = value;
        }
    }
}


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
import java.util.logging.Level;
//    COMMAND <arg1>É<argN> \r\n
//    #M  <s1>É<sM> \r\n
//    B1B2É. BM

public class SocketProtocolReader implements SocketReader {

    final ILogger logger;

    ByteBuffer commandLine = ByteBuffer.allocate(500);
    MutableBoolean commandLineRead = new MutableBoolean(false);

    boolean commandLineIsParsed = false;

    ByteBuffer sizeLine = ByteBuffer.allocate(500);
    MutableBoolean sizeLineRead = new MutableBoolean(false);

    private String command;
    private String[] args;
    private ByteBuffer[] buffers;
    private boolean noreply = false;
    int[] bufferSize;

    final ByteBuffer endOfTheCommand = ByteBuffer.allocate(2);

    private final Connection connection;
    private final IOService ioService;
    private static final String NOREPLY = "noreply";

    private String flag = "0";
    final ByteBuffer firstNewLineRead = ByteBuffer.allocate(2);

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
            while (firstNewLineRead.hasRemaining()) {
                firstNewLineRead.put(bb.get());
            }
            readLine(bb, commandLineRead, commandLine);
            if (commandLineRead.get()) {
                parseCommandLine(SocketTextReader.toStringAndClear(commandLine));
                if (commandLineIsParsed && bufferSize.length > 0) {
                    readLine(bb, sizeLineRead, sizeLine);
                } else if (commandLineIsParsed) {
                    sizeLineRead.set(true);
                }
                if (commandLineIsParsed && bufferSize.length == 0 || sizeLineRead.get()) {
                    //if (bufferSize.length > 0)
                    parseSizeLine(SocketTextReader.toStringAndClear(sizeLine));
                    for (int i = 0; bb.hasRemaining() && i < buffers.length; i++) {
                        if (buffers[i].hasRemaining()) {
                            copy(bb, buffers[i]);
                        }
                        if (i == buffers.length - 1) {
                            if (endOfTheCommand.hasRemaining())
                                copy(bb, endOfTheCommand);
                        }
                    }
                    if ((buffers.length == 0 || !buffers[buffers.length - 1].hasRemaining())) {
                        if (args.length <= 0)
                            throw new RuntimeException("No argument to the command, at least flag should be provided!");
                        Protocol protocol = new Protocol(connection, command, flag, noreply, args, buffers);
                        connection.setType(Connection.Type.PROTOCOL_CLIENT);
                        ioService.handleClientCommand(protocol);
                        reset();
                    }
                }
            }
        } catch (Exception e) {
            connection.getWriteHandler().enqueueSocketWritable(new Protocol(connection,
                    Command.ERROR.value, new String[]{flag, "Malformed_request", e.toString()}));
            logger.log(Level.SEVERE, e.toString(), e);
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
        flag = "0";
        noreply = false;
        buffers = null;
        bufferSize = null;
        commandLineIsParsed = false;
        endOfTheCommand.position(0);
    }

    private void parseSizeLine(String line) {
        if (buffers != null) return;
        System.out.println("Size line is : " + line);
        String[] split = line.split("\\s");
        if (line != "" && split.length != bufferSize.length) {
            throw new RuntimeException("Size # tag and number of size entries do not match!" + split.length + ":: " + bufferSize.length);
        }
        for (int i = 0; i < bufferSize.length; i++) {
            bufferSize[i] = Integer.parseInt(split[i]);
        }
        buffers = new ByteBuffer[bufferSize.length];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocate(bufferSize[i]);
        }
    }

    private boolean parseCommandLine(String line) {
        if (commandLineIsParsed || sizeLineRead.get()) return true;
        if (line.equals("")) {
            commandLineIsParsed = false;
            commandLineRead.set(false);
            return false;
        }
        System.out.println("Command line is : " + line);
        String[] split = line.split("\\s");
        command = split[0];
        int bufferCount = -1;
        int argLength = split.length;
        if (split.length > 0 && split[split.length - 1].startsWith("#")) {
            bufferCount = Integer.parseInt(split[split.length - 1].substring(1));
            noreply = split.length > 1 && NOREPLY.equals(split[split.length - 2]);
        } else {
            noreply = split.length > 1 && NOREPLY.equals(split[split.length - 2]);
        }
        if (bufferCount >= 0) argLength--;
        if (noreply) argLength--;
        flag = split[1];
        args = new String[argLength - 2];
        for (int i = 2; i < argLength; i++) {
            args[i - 2] = split[i];
        }
        if (bufferCount < 0) bufferCount = 0;
        bufferSize = new int[bufferCount];
        commandLineIsParsed = true;
        return true;
    }

    private void readLine(ByteBuffer bb, MutableBoolean lineIsRead, ByteBuffer line) {
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


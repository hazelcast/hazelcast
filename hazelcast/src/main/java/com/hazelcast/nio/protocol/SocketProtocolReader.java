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
import java.util.regex.Pattern;

import static com.hazelcast.nio.Protocol.NOREPLY;
//    COMMAND <arg1>É<argN> \r\n
//    #M  <s1>É<sM> \r\n
//    B1B2É. BM

public class SocketProtocolReader implements SocketReader {

    final ILogger logger;

    ByteBuffer binaryCommandLine = ByteBuffer.allocate(500);
    MutableBoolean commandLineRead = new MutableBoolean(false);

    boolean commandLineIsParsed = false;

    ByteBuffer sizeLine = ByteBuffer.allocate(500);
    MutableBoolean sizeLineRead = new MutableBoolean(false);

    private Command command;
    private int threadId = -1;
    private String[] args;
    private ByteBuffer[] buffers;
    private boolean noreply = false;
    int[] bufferSize;

    final ByteBuffer endOfTheCommand = ByteBuffer.allocate(2);

    private final Connection connection;
    private final IOService ioService;

    private String flag = null;
    final ByteBuffer firstNewLineRead = ByteBuffer.allocate(2);

    Pattern p = Pattern.compile("\\s+");
    Pattern numericPattern = Pattern.compile("([0-9]*)");

    public SocketProtocolReader(Connection connection) {
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
            readLineIfnotRead(bb, commandLineRead, binaryCommandLine);
            if (commandLineIsRead()) {
                String stringCommandLine = SocketTextReader.toStringAndClear(binaryCommandLine);
                parseCommandLine(stringCommandLine);
                if (commandLineIsParsed) {
                    if (hasSizeLine())
                        readLineIfnotRead(bb, sizeLineRead, sizeLine);
                    else
                        sizeLineRead.set(true);
                }
                if (commandLineIsParsed && !hasSizeLine() || sizeLineIsRead()) {
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
                        Protocol protocol = new Protocol(connection, command, flag, threadId, noreply, args, buffers);
                        connection.setType(Connection.Type.PROTOCOL_CLIENT);
                        ioService.handleClientCommand(protocol);
                        reset();
                    }
                }
            }
        } catch (Exception e) {
            connection.getWriteHandler().enqueueSocketWritable(new Protocol(connection,
                    Command.ERROR, flag, threadId, false, new String[]{"Malformed_request", e.toString()}));
            logger.log(Level.SEVERE, e.toString(), e);
        }
    }

    private Boolean sizeLineIsRead() {
        return sizeLineRead.get();
    }

    private boolean hasSizeLine() {
        return bufferSize.length > 0;
    }

    private Boolean commandLineIsRead() {
        return commandLineRead.get();
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
        binaryCommandLine.clear();
        commandLineRead.set(false);
        sizeLine.clear();
        sizeLineRead.set(false);
        command = null;
        threadId = -1;
        args = null;
        flag = null;
        noreply = false;
        buffers = null;
        bufferSize = null;
        commandLineIsParsed = false;
        endOfTheCommand.position(0);
    }

    private void parseSizeLine(String line) {
        if (buffers != null) return;
//        System.out.println("Size line is : " + line);
//        String[] split = line.split("\\s");
        String[] split = fastSplit(line, ' ');
        if (line.length() != 0 && split.length != bufferSize.length) {
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

    public static String[] fastSplit(String line, char split) {
        String[] temp = new String[line.length() / 2 + 1];
        int wordCount = 0;
        int i = 0;
        int j = line.indexOf(split);  // First substring
        while (j >= 0) {
            temp[wordCount++] = line.substring(i, j);
            i = j + 1;
            j = line.indexOf(split, i);   // Rest of substrings
        }
        temp[wordCount++] = line.substring(i); // Last substring
        String[] result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        return result;
    }//end

    private void parseCommandLine(String line) {
        if (commandLineIsParsed || sizeLineIsRead()) return;
        if (line.length() == 0) {
            commandLineIsParsed = false;
            commandLineRead.set(false);
            return;
        }
        String[] split = fastSplit(line, ' ');
        int commandIndex;
        int specialArgCount;
        if (numericPattern.matcher(split[0]).matches()) {
            threadId = Integer.parseInt(split[0]);
            commandIndex = 1;
            flag = split[2];
            specialArgCount = 3;  // THREAD ID, COMMAND, FLAG
        } else {
            commandIndex = 0;
            specialArgCount = 1;  // COMMAND
        }
        try {
            command = Command.valueOf(split[commandIndex]);
        } catch (IllegalArgumentException illegalArgException) {
            command = Command.UNKNOWN;
        }
        int bufferCount = -1;
        int argLength = split.length - specialArgCount;
        if (split.length > 0 && split[split.length - 1].startsWith("#")) {
            bufferCount = Integer.parseInt(split[split.length - 1].substring(1));
            noreply = split.length > 1 && NOREPLY.equals(split[split.length - 2]);
        } else {
            noreply = split.length > 1 && NOREPLY.equals(split[split.length - 1]);
        }
        if (bufferCount >= 0) argLength--;
        if (noreply) argLength--;
        args = new String[argLength];
        for (int i = 0; i < argLength; i++) {
            args[i] = split[i + specialArgCount];
        }
        if (bufferCount < 0) bufferCount = 0;
        bufferSize = new int[bufferCount];
        commandLineIsParsed = true;
    }

    private void readLineIfnotRead(ByteBuffer bb, MutableBoolean lineIsRead, ByteBuffer line) {
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
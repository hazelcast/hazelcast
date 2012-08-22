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

package com.hazelcast.client;

import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.ascii.SocketTextReader;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.protocol.SocketProtocolReader;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class ProtocolReader extends PacketHandler {
    ByteBuffer line = ByteBuffer.allocate(500);
    Pattern numericPattern = Pattern.compile("([0-9]*)");

    public Protocol read(Connection connection) throws IOException {
        int threadId = -1;
        String flag = null;
        Command command;
        String[] args;
        ByteBuffer[] buffers;
        final DataInputStream dis = connection.getInputStream();
        String commandLine = readLine(dis);
        String[] split = SocketProtocolReader.fastSplit(commandLine, ' ');
        if (split.length == 0) {
            throw new RuntimeException("Wrong command from server");
        }
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
        }
        if (bufferCount >= 0) argLength--;
        args = new String[argLength];
        for (int i = 0; i < argLength; i++) {
            args[i] = split[i + specialArgCount];
        }
        if (bufferCount < 0) bufferCount = 0;
        buffers = new ByteBuffer[bufferCount];
        if (bufferCount > 0) {
            String sizeLine = readLine(dis);
            String[] sizes = SocketProtocolReader.fastSplit(sizeLine, ' ');
            int i = 0;
            for (String size : sizes) {
                int length = Integer.parseInt(size);
                byte[] bytes = new byte[length];
                dis.readFully(bytes);
                buffers[i++] = ByteBuffer.wrap(bytes);
            }
            //reading end of the command;
            dis.readByte();
            dis.readByte();
        }
        if (command.equals(Command.UNKNOWN)) {
            throw new RuntimeException("Unknown command: " + split[commandIndex]);
        }
        Protocol protocol = new Protocol(null, command, flag, threadId, false, args, buffers);
        return protocol;
    }

    private String readLine(DataInputStream dis) throws IOException {
        byte b = dis.readByte();
        char c = (char) b;
        while (c != '\n') {
            System.out.println(c);
            if (c != '\r')
                line.put(b);
            b = dis.readByte();
            c = (char) dis.readByte();
        }
        return SocketTextReader.toStringAndClear(line);
    }
}

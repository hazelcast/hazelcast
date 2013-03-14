/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.ascii.SocketTextReader;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.protocol.SocketProtocolReader;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.ObjectDataInputStream;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.DataInput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ProtocolReader {
    private final Pattern numericPattern = Pattern.compile("([0-9]*)");
    private final SerializationService serializationService;

    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    public ProtocolReader(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public Protocol read(Connection connection) throws IOException {
        ByteBuffer line = ByteBuffer.allocate(INITIAL_BUFFER_CAPACITY);
        int threadId = -1;
        String flag = null;
        Command command;
        String[] args;
        final ObjectDataInputStream dis = connection.getInputStream();
        String commandLine = readLine(dis, line);
//        System.out.println("Commandline is " + commandLine);
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
        Data[] datas = new Data[bufferCount];
        if (bufferCount > 0) {
            if (bufferCount * 11 > line.array().length) {
                line = ByteBuffer.allocate(bufferCount * 11);
            }
            String sizeLine = readLine(dis, line);
//            System.out.println("Size line is " + sizeLine);
            String[] sizes = SocketProtocolReader.fastSplit(sizeLine, ' ');
            int i = 0;
            for (String size : sizes) {
                int length = Integer.parseInt(size);
                byte[] bytes = new byte[length];
                dis.readFully(bytes);
                datas[i] = new Data();
                datas[i].readData(serializationService.createObjectDataInput(bytes));
//                System.out.println(command + "datas " + i + ": " +datas[i]);
                i++;
            }
            dis.readByte();
            dis.readByte();
        }
        if (command.equals(Command.UNKNOWN)) {
            throw new RuntimeException("Unknown command: " + split[commandIndex] + ":: "+ Thread.currentThread().getName());
        }
        Protocol protocol = new Protocol(null, command, flag, threadId, false, args, datas);
        return protocol;
    }

    private String readLine(DataInput dis, ByteBuffer line) throws IOException {
        byte b = dis.readByte();
        char c = (char) b;
        while (c != '\n') {
            if (c != '\r')
                try{
                    line.put(b);
                }catch (BufferOverflowException boe){
                    ByteBuffer _line = ByteBuffer.allocate(line.capacity()*2);
                    _line.put(line.array());
                    _line.put(b);
                    line = _line;
                }
            b = dis.readByte();
            c = (char) b;
        }
        String result = SocketTextReader.toStringAndClear(line);
        if(line.capacity() > INITIAL_BUFFER_CAPACITY*10) line = ByteBuffer.allocate(INITIAL_BUFFER_CAPACITY);
        return result;
    }

    public Protocol read(Connection connection, long timeout, TimeUnit unit) throws IOException {
        return null;
    }
}

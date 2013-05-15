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

package com.hazelcast.deprecated.nio;

import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;

import java.io.*;
import java.nio.ByteBuffer;

public class Protocol implements SocketWritable {

    public static final String NOREPLY = "noreply";
    private static final String NEWLINE = "\r\n";
    private static final String SPACE = " ";
    public final Command command;
    public final String[] args;
    public final Data[] buffers;
    public final TcpIpConnection conn;
    public final boolean noReply;
    public String flag;
    public int threadId;

    private final BufferWriterIterator writerIterator = new BufferWriterIterator();
    private ByteBuffer response = null;
    private int totalSize = 0;
    private int totalWritten = 0;

    public Protocol(TcpIpConnection connection, Command command, String[] args, Data... buffers) {
        this(connection, command, null, -1, false, args, buffers);
    }

    public Protocol(TcpIpConnection connection, Command command, String flag, int threadId, boolean noReply, String[] args, Data... buffers) {
        this.buffers = buffers;
        this.args = args;
        this.command = command;
        this.conn = connection;
        this.noReply = noReply;
        this.flag = flag;
        this.threadId = threadId;
    }

    public void onEnqueue() {
        StringBuilder builder = new StringBuilder();
        if (threadId != -1)
            builder.append(threadId).append(SPACE);
        builder.append(command.toString());
        if (flag != null) {
            builder.append(SPACE).append(flag);
        }
        for (String arg : args) {
            builder.append(SPACE).append(arg);
        }
        if (hasBuffer()) {
            builder.append(SPACE).append("#").append(buffers == null ? 0 : buffers.length);
            builder.append(NEWLINE);
            int i = buffers.length;
            for (Data buffer : buffers) {
                builder.append(buffer == null ? 0 : buffer.totalSize());
                if (--i != 0) {
                    builder.append(SPACE);
                }
            }
        }
        builder.append(NEWLINE);
        response = ByteBuffer.wrap(builder.toString().getBytes());
        totalSize = response.array().length;
        if (hasBuffer()) {
            // don't take buffer sizes' into account!
            // we'll use Data's own writer mechanism to detect
            // if buffers are completely written to destination.

//            for (Data buffer : buffers) {
//                totalSize += buffer.totalSize();
//            }
            totalSize += 2;
        }
    }

    public boolean writeTo(ByteBuffer destination) {
        totalWritten += IOUtil.copyToHeapBuffer(response, destination);
        if (hasBuffer()) {
            while (writerIterator.hasNext()) {
                DataAdapter dw = writerIterator.next();
                if (!dw.writeTo(destination)) { // check if buffer is written completely
                    return false;
                }
            }
            totalWritten += IOUtil.copyToHeapBuffer(ByteBuffer.wrap(NEWLINE.getBytes()), destination);
        }
        return totalWritten >= totalSize;
    }

    public final boolean writeTo(final ObjectDataOutput dos) throws IOException {
        dos.write(response.array());
        if (buffers != null && buffers.length > 0) {
            for (Data buffer : buffers) {
                if(buffer!=null)
                    buffer.writeData(dos);
            }
            dos.write(NEWLINE.getBytes());
        }
        return true;
    }

    public Protocol success(String... args) {
        return success((Data) null, args);
    }

    public Protocol success(Data buffer, String... args) {
        int size = buffer == null ? 0 : 1;
        Data[] buffers = new Data[size];
        if (size > 0) buffers[0] = buffer;
        return success(buffers, args);
    }

    public Protocol success(Data[] buffers, String... args) {
        return create(Command.OK, args, buffers);
    }

    public Protocol error(Data[] buffers, String error) {
        return create(Command.ERROR, new String[]{error}, buffers);
    }

    public Protocol create(Command command, String[] args, Data... buffers) {
        if (args == null) args = new String[]{};
        return new Protocol(this.conn, command, this.flag, this.threadId, this.noReply, args, buffers);
    }

    @Override
    public String toString() {
        return "Protocol{" +
                "command=" + command +
                '}';
    }

    public boolean hasBuffer() {
        return buffers != null && buffers.length > 0;
    }

    private class BufferWriterIterator {
        int index = 0;
        DataAdapter current = null;

        boolean hasNext() {
            return hasBuffer() &&
                    (hasCurrent() || buffers.length > index);
        }

        boolean hasCurrent() {
            return current != null && !current.done();
        }

        DataAdapter next() {
            if (hasCurrent()) {
                return current;
            }

            Data data = buffers[index++];
            current = new DataAdapter(data);
            return current;
        }
    }
}

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

package com.hazelcast.nio;

import com.hazelcast.nio.protocol.Command;

import java.nio.ByteBuffer;

public class Protocol implements SocketWritable {

    public static final String NOREPLY = "noreply";
    public final Command command;
    public final String[] args;
    public final ByteBuffer[] buffers;
    public final Connection conn;
    public final boolean noReply;
    public String flag;
    public int threadId;

    ByteBuffer response = null;
    int totalSize = 0;
    int totalWritten = 0;

    public Protocol(Connection connection, Command command, String[] args, ByteBuffer... buffers) {
        this(connection, command, null, -1, false, args, buffers);
    }

    public Protocol(Connection connection, Command command, String flag, int threadId, boolean noReply, String[] args, ByteBuffer... buffers) {
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
            builder.append(threadId).append(" ");
        builder.append(command.toString());
        if (flag != null) {
            builder.append(" ").append(flag);
        }
        for (String arg : args) {
            builder.append(" ").append(arg);
        }
        int bSize = buffers == null ? 0 : buffers.length;
        if (bSize > 0) {
            builder.append(" #").append(buffers == null ? 0 : buffers.length);
            builder.append("\r\n");
            int i = buffers.length;
            for (ByteBuffer buffer : buffers) {
                builder.append(buffer == null ? 0 : buffer.capacity());
                if (--i != 0) {
                    builder.append(" ");
                }
            }
        }
        builder.append("\r\n");
        response = ByteBuffer.wrap(builder.toString().getBytes());
        totalSize = response.array().length;
        if (buffers != null && buffers.length > 0) {
            for (ByteBuffer buffer : buffers) {
                totalSize += buffer.capacity();
            }
            totalSize += 2;
        }
    }

    public final boolean writeToSocketBuffer(ByteBuffer destination) {
        totalWritten += IOUtil.copyToHeapBuffer(response, destination);
        if (buffers != null && buffers.length > 0) {
            for (ByteBuffer buffer : buffers) {
                totalWritten += IOUtil.copyToHeapBuffer(buffer, destination);
            }
            totalWritten += IOUtil.copyToHeapBuffer(ByteBuffer.wrap("\r\n".getBytes()), destination);
        }
        return totalWritten >= totalSize;
    }

    public Protocol success(String... args) {
        return success((ByteBuffer) null, args);
    }

    public Protocol success(ByteBuffer buffer, String... args) {
        int size = buffer == null ? 0 : 1;
        ByteBuffer[] buffers = new ByteBuffer[size];
        if (size > 0) buffers[0] = buffer;
        return success(buffers, args);
    }

    public Protocol success(ByteBuffer[] buffers, String... args) {
        return create(Command.OK, args, buffers);
    }

    public Protocol error(ByteBuffer[] buffers, String... args) {
        return create(Command.ERROR, args, buffers);
    }

    public Protocol create(Command command, String[] args, ByteBuffer... buffers) {
        if (args == null) args = new String[]{};
        return new Protocol(this.conn, command, this.flag, this.threadId, this.noReply, args, buffers);
    }

    @Override
    public String toString() {
        return "Protocol{" +
                "command=" + command +
                '}';
    }
}

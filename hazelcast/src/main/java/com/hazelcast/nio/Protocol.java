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
    private static final String NOREPLY = "noreply";
    private final String command;
    private final String[] args;
    private final ByteBuffer[] buffers;
    private final Connection connection;
    private final boolean noReply;
    private long flag;

    ByteBuffer response = null;
    int totalSize = 0;
    int totalWritten = 0;

    public Protocol(Connection connection, String command, String[] args, ByteBuffer... buffers) {
        this(connection, command, Long.valueOf(args[0]), (args != null && args.length > 0 && NOREPLY.equals(args[args.length - 1])), args, buffers);
    }

    public Protocol(Connection connection, String command, Long flag, boolean noReply, String[] args, ByteBuffer... buffers) {
        this.buffers = buffers;
        this.args = args;
        this.command = command;
        this.connection = connection;
        this.noReply = noReply;
        this.flag = flag;
    }

    public String getCommand() {
        return command;
    }

    public String[] getArgs() {
        return args;
    }

    public ByteBuffer[] getBuffers() {
        return buffers;
    }

    public Connection getConnection() {
        return connection;
    }

    public long getFlag() {
        return flag;
    }

    public boolean isNoReply() {
        return noReply;
    }

    public void onEnqueue() {
        StringBuilder builder = new StringBuilder(command);
        for (String arg : args) {
            builder.append(" ").append(arg);
        }
        builder.append("\r\n");
        builder.append("#").append(buffers == null ? 0 : buffers.length);
        if (buffers != null){
            for (ByteBuffer buffer : buffers) {
                builder.append(" ").append(buffer == null ? 0 : buffer.capacity());
            }
        }
        builder.append("\r\n");
        response = ByteBuffer.wrap(builder.toString().getBytes());
        totalSize = response.array().length;
        if (buffers != null && buffers.length > 0) {
            for (ByteBuffer buffer : buffers) {
                totalSize += buffer.capacity();
            }
        }
    }

    public final boolean writeToSocketBuffer(ByteBuffer destination) {
        totalWritten += IOUtil.copyToHeapBuffer(response, destination);
        if (buffers != null && buffers.length > 0) {
            for (ByteBuffer buffer : buffers) {
                totalWritten += IOUtil.copyToHeapBuffer(buffer, destination);
            }
        }
        return totalWritten >= totalSize;
    }

    public Protocol success(String... args) {
        return success((ByteBuffer)null, args);
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
        String[] argWithFlag = new String[args.length + 1];
        argWithFlag[0] = String.valueOf(this.flag);
        System.arraycopy(args, 0, argWithFlag, 1, args.length);
        return new Protocol(this.connection, command.value, this.flag, this.noReply, argWithFlag, buffers);
    }
}

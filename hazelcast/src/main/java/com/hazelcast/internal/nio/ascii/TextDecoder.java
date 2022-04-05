/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.ascii;

import com.hazelcast.internal.ascii.CommandParser;
import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.memcache.ErrorCommand;
import com.hazelcast.internal.ascii.rest.HttpCommand;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.UNKNOWN;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.util.JVMUtil.upcast;

public abstract class TextDecoder extends InboundHandler<ByteBuffer, Void> {

    @SuppressWarnings("checkstyle:magicnumber")
    private static final int INITIAL_CAPACITY = 1 << 8;
    // 65536, no specific reason, similar to UDP packet size limit
    @SuppressWarnings("checkstyle:magicnumber")
    private static final int MAX_CAPACITY = 1 << 16;

    private ByteBuffer commandLineBuffer = ByteBuffer.allocate(INITIAL_CAPACITY);
    private boolean commandLineRead;
    private TextCommand command;
    private final TextCommandService textCommandService;
    private final TextEncoder encoder;
    private final ServerConnection connection;
    private boolean connectionTypeSet;
    private long requestIdGen;
    private final TextProtocolFilter textProtocolFilter;
    private final ILogger logger;
    private final TextParsers textParsers;
    private final boolean rootDecoder;

    public TextDecoder(ServerConnection connection, TextEncoder encoder, TextProtocolFilter textProtocolFilter,
                       TextParsers textParsers, boolean rootDecoder) {
        ServerContext serverContext = connection.getConnectionManager().getServer().getContext();
        this.textCommandService = serverContext.getTextCommandService();
        this.encoder = encoder;
        this.connection = connection;
        this.textProtocolFilter = textProtocolFilter;
        this.textParsers = textParsers;
        this.logger = serverContext.getLoggingService().getLogger(getClass());
        this.rootDecoder = rootDecoder;
    }

    public void sendResponse(TextCommand command) {
        encoder.enqueue(command);
    }

    @Override
    public void handlerAdded() {
        if (rootDecoder) {
            initSrcBuffer();
        }
    }

    @Override
    public HandlerStatus onRead() throws Exception {
        upcast(src).flip();
        try {
            while (src.hasRemaining()) {
                doRead(src);
            }

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    private void doRead(ByteBuffer bb) throws IOException {
        while (!commandLineRead && bb.hasRemaining()) {
            byte b = bb.get();
            char c = (char) b;
            if (c == '\n') {
                commandLineRead = true;
            } else if (c != '\r') {
                appendToBuffer(b);
            }
        }
        if (commandLineRead) {
            if (command == null) {
                String commandLine = toStringAndClear(commandLineBuffer);
                // evaluate the command immediately - close connection if command is unknown or not enabled
                textProtocolFilter.filterConnection(commandLine, connection);
                if (!connection.isAlive()) {
                    reset();
                    return;
                }
                processCmd(commandLine);
            }
            if (command != null) {
                boolean complete = command.readFrom(bb);
                if (complete) {
                    publishRequest(command);
                    reset();
                }
            } else {
                reset();
            }
        }
    }

    private void appendToBuffer(byte b) throws IOException {
        if (!commandLineBuffer.hasRemaining()) {
            expandBuffer();
        }
        commandLineBuffer.put(b);
    }

    private void expandBuffer() throws IOException {
        if (commandLineBuffer.capacity() == MAX_CAPACITY) {
            throw new IOException("Max command size capacity [" + MAX_CAPACITY + "] has been reached!");
        }

        int capacity = commandLineBuffer.capacity() << 1;
        if (logger.isFineEnabled()) {
            logger.fine("Expanding buffer capacity to " + capacity);
        }

        ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
        upcast(commandLineBuffer).flip();
        newBuffer.put(commandLineBuffer);
        commandLineBuffer = newBuffer;
    }

    private void reset() {
        command = null;
        upcast(commandLineBuffer).clear();
        commandLineRead = false;
    }

    private static String toStringAndClear(ByteBuffer bb) {
        if (bb == null) {
            return "";
        }
        String result;
        if (bb.position() == 0) {
            result = "";
        } else {
            result = StringUtil.bytesToString(bb.array(), 0, bb.position());
        }
        upcast(bb).clear();
        return result;
    }

    public void publishRequest(TextCommand command) {
        if (!isCommandTypeEnabled(command)) {
            return;
        }
        long requestId = (command.shouldReply()) ? requestIdGen++ : -1;
        command.init(this, requestId);
        textCommandService.processRequest(command);
    }

    private boolean isCommandTypeEnabled(TextCommand command) {
        if (!connectionTypeSet) {
            if (command instanceof HttpCommand) {
                connection.setConnectionType(ConnectionType.REST_CLIENT);
            } else {
                connection.setConnectionType(ConnectionType.MEMCACHE_CLIENT);
            }
            connectionTypeSet = true;
        }
        return true;
    }

    private void processCmd(String cmd) {
        try {
            int space = cmd.indexOf(' ');
            String operation = (space == -1) ? cmd : cmd.substring(0, space);
            CommandParser commandParser = textParsers.getParser(operation);
            if (commandParser != null) {
                command = commandParser.parser(this, cmd, space);
            } else {
                command = new ErrorCommand(UNKNOWN);
            }
        } catch (Throwable t) {
            logger.finest(t);
            command = new ErrorCommand(ERROR_CLIENT, "Invalid command: " + cmd);
        }
    }

    public TextEncoder getEncoder() {
        return encoder;
    }

    public void closeConnection() {
        connection.close(null, null);
    }

    public ServerConnection getConnection() {
        return connection;
    }
}

/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.ascii;

import com.hazelcast.internal.ascii.CommandParser;
import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.memcache.DeleteCommandParser;
import com.hazelcast.internal.ascii.memcache.ErrorCommand;
import com.hazelcast.internal.ascii.memcache.GetCommandParser;
import com.hazelcast.internal.ascii.memcache.IncrementCommandParser;
import com.hazelcast.internal.ascii.memcache.SetCommandParser;
import com.hazelcast.internal.ascii.memcache.SimpleCommandParser;
import com.hazelcast.internal.ascii.memcache.TouchCommandParser;
import com.hazelcast.internal.ascii.rest.HttpCommand;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpDeleteCommandParser;
import com.hazelcast.internal.ascii.rest.HttpGetCommandParser;
import com.hazelcast.internal.ascii.rest.HttpPostCommandParser;
import com.hazelcast.internal.networking.ReadHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.StringUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ADD;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.APPEND;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.DECREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.INCREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.PREPEND;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.REPLACE;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.SET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.STATS;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.TOUCH;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.UNKNOWN;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.VERSION;

@PrivateApi
public class TextReadHandler implements ReadHandler {

    private static final Map<String, CommandParser> MAP_COMMAND_PARSERS = new HashMap<String, CommandParser>();

    @SuppressWarnings("checkstyle:magicnumber")
    private static final int INITIAL_CAPACITY = 1 << 8;
    // 65536, no specific reason, similar to UDP packet size limit
    @SuppressWarnings("checkstyle:magicnumber")
    private static final int MAX_CAPACITY = 1 << 16;

    static {
        MAP_COMMAND_PARSERS.put("get", new GetCommandParser());
        MAP_COMMAND_PARSERS.put("gets", new GetCommandParser());
        MAP_COMMAND_PARSERS.put("set", new SetCommandParser(SET));
        MAP_COMMAND_PARSERS.put("add", new SetCommandParser(ADD));
        MAP_COMMAND_PARSERS.put("replace", new SetCommandParser(REPLACE));
        MAP_COMMAND_PARSERS.put("append", new SetCommandParser(APPEND));
        MAP_COMMAND_PARSERS.put("prepend", new SetCommandParser(PREPEND));
        MAP_COMMAND_PARSERS.put("touch", new TouchCommandParser(TOUCH));
        MAP_COMMAND_PARSERS.put("incr", new IncrementCommandParser(INCREMENT));
        MAP_COMMAND_PARSERS.put("decr", new IncrementCommandParser(DECREMENT));
        MAP_COMMAND_PARSERS.put("delete", new DeleteCommandParser());
        MAP_COMMAND_PARSERS.put("quit", new SimpleCommandParser(QUIT));
        MAP_COMMAND_PARSERS.put("stats", new SimpleCommandParser(STATS));
        MAP_COMMAND_PARSERS.put("version", new SimpleCommandParser(VERSION));
        MAP_COMMAND_PARSERS.put("GET", new HttpGetCommandParser());
        MAP_COMMAND_PARSERS.put("POST", new HttpPostCommandParser());
        MAP_COMMAND_PARSERS.put("PUT", new HttpPostCommandParser());
        MAP_COMMAND_PARSERS.put("DELETE", new HttpDeleteCommandParser());
    }

    private ByteBuffer commandLineBuffer = ByteBuffer.allocate(INITIAL_CAPACITY);
    private boolean commandLineRead;
    private TextCommand command;
    private final TextCommandService textCommandService;
    private final TextWriteHandler textWriteHandler;
    private final TcpIpConnection connection;
    private final boolean restEnabled;
    private final boolean memcacheEnabled;
    private final boolean healthcheckEnabled;
    private boolean connectionTypeSet;
    private long requestIdGen;
    private final ILogger logger;

    public TextReadHandler(TcpIpConnection connection) {
        IOService ioService = connection.getConnectionManager().getIoService();
        this.textCommandService = ioService.getTextCommandService();
        this.textWriteHandler = (TextWriteHandler) connection.getSocketWriter().getWriteHandler();
        this.connection = connection;
        this.memcacheEnabled = ioService.isMemcacheEnabled();
        this.restEnabled = ioService.isRestEnabled();
        this.healthcheckEnabled = ioService.isHealthcheckEnabled();
        this.logger = ioService.getLoggingService().getLogger(getClass());
    }

    public void sendResponse(TextCommand command) {
        textWriteHandler.enqueue(command);
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        while (src.hasRemaining()) {
            doRead(src);
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
                processCmd(toStringAndClear(commandLineBuffer));
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
        commandLineBuffer.flip();
        newBuffer.put(commandLineBuffer);
        commandLineBuffer = newBuffer;
    }

    private void reset() {
        command = null;
        commandLineBuffer.clear();
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
        bb.clear();
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
                String uri = ((HttpCommand) command).getURI();
                boolean isMancenterRequest = uri.startsWith(HttpCommandProcessor.URI_MANCENTER_CHANGE_URL);
                boolean isClusterManagementRequest = uri.startsWith(HttpCommandProcessor.URI_CLUSTER_MANAGEMENT_BASE_URL);
                boolean isHealthCheck = healthcheckEnabled && uri.startsWith(HttpCommandProcessor.URI_HEALTH_URL);
                boolean forceRequestHandling = isClusterManagementRequest || isMancenterRequest || isHealthCheck;
                if (!restEnabled && !forceRequestHandling) {
                    connection.close("REST not enabled", null);
                    return false;
                }
                connection.setType(ConnectionType.REST_CLIENT);
            } else {
                if (!memcacheEnabled) {
                    connection.close("Memcached not enabled", null);
                    return false;
                }
                connection.setType(ConnectionType.MEMCACHE_CLIENT);
            }
            connectionTypeSet = true;
        }
        return true;
    }

    private void processCmd(String cmd) {
        try {
            int space = cmd.indexOf(' ');
            String operation = (space == -1) ? cmd : cmd.substring(0, space);
            CommandParser commandParser = MAP_COMMAND_PARSERS.get(operation);
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

    public TextWriteHandler getTextWriteHandler() {
        return textWriteHandler;
    }

    public void closeConnection() {
        connection.close(null, null);
    }
}

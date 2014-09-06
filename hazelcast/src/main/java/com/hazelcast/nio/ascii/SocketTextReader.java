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

package com.hazelcast.nio.ascii;

import com.hazelcast.ascii.CommandParser;
import com.hazelcast.ascii.TextCommand;
import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.ascii.memcache.GetCommandParser;
import com.hazelcast.ascii.memcache.SetCommandParser;
import com.hazelcast.ascii.memcache.DeleteCommandParser;
import com.hazelcast.ascii.memcache.IncrementCommandParser;
import com.hazelcast.ascii.memcache.SimpleCommandParser;
import com.hazelcast.ascii.memcache.TouchCommandParser;
import com.hazelcast.ascii.memcache.ErrorCommand;
import com.hazelcast.ascii.rest.HttpCommand;
import com.hazelcast.ascii.rest.HttpDeleteCommandParser;
import com.hazelcast.ascii.rest.HttpGetCommandParser;
import com.hazelcast.ascii.rest.HttpPostCommandParser;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.ADD;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.APPEND;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.INCREMENT;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.REPLACE;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.SET;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.DECREMENT;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.PREPEND;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.TOUCH;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.STATS;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.VERSION;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.UNKNOWN;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;

public class SocketTextReader implements SocketReader {

    private static final Map<String, CommandParser> MAP_COMMAND_PARSERS = new HashMap<String, CommandParser>();

    private static final int CAPACITY = 500;

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

    private ByteBuffer commandLine = ByteBuffer.allocate(CAPACITY);
    private boolean commandLineRead;
    private TextCommand command;
    private final TextCommandService textCommandService;
    private final SocketTextWriter socketTextWriter;
    private final TcpIpConnection connection;
    private final boolean restEnabled;
    private final boolean memcacheEnabled;
    private boolean connectionTypeSet;
    private long requestIdGen;
    private final ILogger logger;

    public SocketTextReader(TcpIpConnection connection) {
        IOService ioService = connection.getConnectionManager().getIOHandler();
        this.textCommandService = ioService.getTextCommandService();
        this.socketTextWriter = (SocketTextWriter) connection.getWriteHandler().getSocketWriter();
        this.connection = connection;
        this.memcacheEnabled = ioService.isMemcacheEnabled();
        this.restEnabled = ioService.isRestEnabled();
        this.logger = ioService.getLogger(this.getClass().getName());
    }

    public void sendResponse(TextCommand command) {
        socketTextWriter.enqueue(command);
    }

    public void read(ByteBuffer inBuffer) {
        while (inBuffer.hasRemaining()) {
            doRead(inBuffer);
        }
    }

    private void doRead(ByteBuffer bb) {
        while (!commandLineRead && bb.hasRemaining()) {
            byte b = bb.get();
            char c = (char) b;
            if (c == '\n') {
                commandLineRead = true;
            } else if (c != '\r') {
                commandLine.put(b);
            }
        }
        if (commandLineRead) {
            if (command == null) {
                processCmd(toStringAndClear(commandLine));
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

    void reset() {
        command = null;
        commandLine.clear();
        commandLineRead = false;
    }

    public static String toStringAndClear(ByteBuffer bb) {
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
        if (!connectionTypeSet) {
            if (command instanceof HttpCommand) {
                if (!restEnabled) {
                    connection.close();
                }
                connection.setType(ConnectionType.REST_CLIENT);
            } else {
                if (!memcacheEnabled) {
                    connection.close();
                }
                connection.setType(ConnectionType.MEMCACHE_CLIENT);
            }
            connectionTypeSet = true;
        }
        long requestId = (command.shouldReply()) ? requestIdGen++ : -1;
        command.init(this, requestId);
        textCommandService.processRequest(command);
    }

    void processCmd(String cmd) {
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
            command = new ErrorCommand(ERROR_CLIENT, "Invalid command : " + cmd);
        }
    }

    public SocketTextWriter getSocketTextWriter() {
        return socketTextWriter;
    }

    public void closeConnection() {
        connection.close();
    }
}

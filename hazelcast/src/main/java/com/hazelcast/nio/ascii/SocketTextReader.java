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
import com.hazelcast.ascii.TextCommandConstants;
import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.ascii.memcache.*;
import com.hazelcast.ascii.rest.HttpCommand;
import com.hazelcast.ascii.rest.HttpDeleteCommandParser;
import com.hazelcast.ascii.rest.HttpGetCommandParser;
import com.hazelcast.ascii.rest.HttpPostCommandParser;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.SocketReader;
import com.hazelcast.nio.TcpIpConnection;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.*;

public class SocketTextReader implements TextCommandConstants, SocketReader {

    private final static Map<String, CommandParser> mapCommandParsers = new HashMap<String, CommandParser>();

    static {
        mapCommandParsers.put("get", new GetCommandParser());
        mapCommandParsers.put("gets", new GetCommandParser());
        mapCommandParsers.put("set", new SetCommandParser(SET));
        mapCommandParsers.put("add", new SetCommandParser(ADD));
        mapCommandParsers.put("replace", new SetCommandParser(REPLACE));
        mapCommandParsers.put("append", new SetCommandParser(APPEND));
        mapCommandParsers.put("prepend", new SetCommandParser(PREPEND));
        mapCommandParsers.put("touch", new TouchCommandParser(TOUCH));
        mapCommandParsers.put("incr", new IncrementCommandParser(INCREMENT));
        mapCommandParsers.put("decr", new IncrementCommandParser(DECREMENT));
        mapCommandParsers.put("delete", new DeleteCommandParser());
        mapCommandParsers.put("quit", new SimpleCommandParser(QUIT));
        mapCommandParsers.put("stats", new SimpleCommandParser(STATS));
        mapCommandParsers.put("version", new SimpleCommandParser(VERSION));
        mapCommandParsers.put("GET", new HttpGetCommandParser());
        mapCommandParsers.put("POST", new HttpPostCommandParser());
        mapCommandParsers.put("PUT", new HttpPostCommandParser());
        mapCommandParsers.put("DELETE", new HttpDeleteCommandParser());
    }

    private ByteBuffer commandLine = ByteBuffer.allocate(500);
    private boolean commandLineRead = false;
    private TextCommand command = null;
    private final TextCommandService textCommandService;
    private final SocketTextWriter socketTextWriter;
    private final TcpIpConnection connection;
    private final boolean restEnabled;
    private final boolean memcacheEnabled;
    private boolean connectionTypeSet = false;
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
        if (bb == null) return "";
        String result = null;
        if (bb.position() == 0) {
            result = "";
        } else {
            result = new String(bb.array(), 0, bb.position());
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
            CommandParser commandParser = mapCommandParsers.get(operation);
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

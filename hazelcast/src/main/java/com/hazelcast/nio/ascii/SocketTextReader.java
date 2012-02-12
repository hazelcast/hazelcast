/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.impl.ascii.CommandParser;
import com.hazelcast.impl.ascii.TextCommand;
import com.hazelcast.impl.ascii.TextCommandConstants;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.impl.ascii.memcache.*;
import com.hazelcast.impl.ascii.rest.HttpCommand;
import com.hazelcast.impl.ascii.rest.HttpDeleteCommandParser;
import com.hazelcast.impl.ascii.rest.HttpGetCommandParser;
import com.hazelcast.impl.ascii.rest.HttpPostCommandParser;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.SocketReader;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import static com.hazelcast.impl.ascii.TextCommandConstants.TextCommandType.*;

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
        mapCommandParsers.put("delete", new DeleteCommandParser());
        mapCommandParsers.put("quit", new SimpleCommandParser(QUIT));
        mapCommandParsers.put("stats", new SimpleCommandParser(STATS));
        mapCommandParsers.put("GET", new HttpGetCommandParser());
        mapCommandParsers.put("POST", new HttpPostCommandParser());
        mapCommandParsers.put("PUT", new HttpPostCommandParser());
        mapCommandParsers.put("DELETE", new HttpDeleteCommandParser());
    }

    ByteBuffer commandLine = ByteBuffer.allocate(500);
    boolean commandLineRead = false;
    TextCommand command = null;
    private final TextCommandService textCommandService;
    private final SocketTextWriter socketTextWriter;
    private final Connection connection;
    private final boolean restEnabled;
    private final boolean memcacheEnabled;
    boolean connectionTypeSet = false;
    long requestIdGen;
    private final ILogger logger;

    public SocketTextReader(Connection connection) {
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
                boolean complete = command.doRead(bb);
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
//        System.out.println("publishing " + command);
        if (!connectionTypeSet) {
            if (command instanceof HttpCommand) {
                if (!restEnabled) {
                    connection.close();
                }
                connection.setType(Connection.Type.REST_CLIENT);
            } else {
                if (!memcacheEnabled) {
                    connection.close();
                }
                connection.setType(Connection.Type.MEMCACHE_CLIENT);
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
            logger.log(Level.FINEST, t.getMessage(), t);
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

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
import com.hazelcast.internal.ascii.rest.HttpDeleteCommandParser;
import com.hazelcast.internal.ascii.rest.HttpGetCommandParser;
import com.hazelcast.internal.ascii.rest.HttpHeadCommandParser;
import com.hazelcast.internal.ascii.rest.HttpPostCommandParser;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.ServerConnection;

import java.util.HashMap;
import java.util.Map;

public class RestApiTextDecoder extends TextDecoder {

    public static final TextParsers TEXT_PARSERS;

    static {
        Map<String, CommandParser> parsers = new HashMap<String, CommandParser>();
        parsers.put("GET", new HttpGetCommandParser());
        parsers.put("POST", new HttpPostCommandParser());
        parsers.put("PUT", new HttpPostCommandParser());
        parsers.put("DELETE", new HttpDeleteCommandParser());
        parsers.put("HEAD", new HttpHeadCommandParser());
        TEXT_PARSERS = new TextParsers(parsers);
    }

    public RestApiTextDecoder(ServerConnection connection, TextEncoder encoder, boolean rootDecoder) {
        super(connection, encoder, createFilter(connection), TEXT_PARSERS, rootDecoder);
    }

    private static RestApiFilter createFilter(ServerConnection connection) {
        ServerContext serverContext = connection.getConnectionManager().getServer().getContext();
        return new RestApiFilter(serverContext.getLoggingService(), serverContext.getRestApiConfig(), TEXT_PARSERS);
    }
}

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
import com.hazelcast.internal.ascii.memcache.DeleteCommandParser;
import com.hazelcast.internal.ascii.memcache.GetCommandParser;
import com.hazelcast.internal.ascii.memcache.IncrementCommandParser;
import com.hazelcast.internal.ascii.memcache.SetCommandParser;
import com.hazelcast.internal.ascii.memcache.SimpleCommandParser;
import com.hazelcast.internal.ascii.memcache.TouchCommandParser;
import com.hazelcast.internal.server.ServerConnection;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ADD;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.APPEND;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.DECREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.INCREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.PREPEND;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.REPLACE;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.SET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.STATS;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.TOUCH;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.VERSION;

public class MemcacheTextDecoder extends TextDecoder {

    public static final TextParsers TEXT_PARSERS;

    static {
        Map<String, CommandParser> parsers = new HashMap<String, CommandParser>();
        parsers.put("get", new GetCommandParser());
        parsers.put("gets", new GetCommandParser());
        parsers.put("set", new SetCommandParser(SET));
        parsers.put("add", new SetCommandParser(ADD));
        parsers.put("replace", new SetCommandParser(REPLACE));
        parsers.put("append", new SetCommandParser(APPEND));
        parsers.put("prepend", new SetCommandParser(PREPEND));
        parsers.put("touch", new TouchCommandParser(TOUCH));
        parsers.put("incr", new IncrementCommandParser(INCREMENT));
        parsers.put("decr", new IncrementCommandParser(DECREMENT));
        parsers.put("delete", new DeleteCommandParser());
        parsers.put("quit", new SimpleCommandParser(QUIT));
        parsers.put("stats", new SimpleCommandParser(STATS));
        parsers.put("version", new SimpleCommandParser(VERSION));
        TEXT_PARSERS = new TextParsers(parsers);
    }

    public MemcacheTextDecoder(ServerConnection connection, TextEncoder encoder, boolean rootDecoder) {
        super(connection, encoder, AllowingTextProtocolFilter.INSTANCE, TEXT_PARSERS, rootDecoder);
    }
}

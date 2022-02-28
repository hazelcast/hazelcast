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
import com.hazelcast.internal.nio.Protocols;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TextParsers {

    private final Map<String, CommandParser> parsers;
    private final Set<String> commandPrefixes;

    public TextParsers(Map<String, CommandParser> parsers) {
        this.parsers = new HashMap<String, CommandParser>(parsers);
        Set<String> prefixes = new HashSet<String>();
        for (String command : parsers.keySet()) {
            prefixes.add(command.substring(0, Protocols.PROTOCOL_LENGTH));
        }
        this.commandPrefixes = prefixes;
    }

    public CommandParser getParser(String command) {
        return command != null ? parsers.get(command) : null;
    }

    public boolean isCommandPrefix(String prefix) {
        return prefix != null ? commandPrefixes.contains(prefix) : false;
    }
}

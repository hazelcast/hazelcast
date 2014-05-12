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

package com.hazelcast.ascii.memcache;

import com.hazelcast.ascii.TextCommand;
import com.hazelcast.ascii.TextCommandConstants;
import com.hazelcast.ascii.TypeAwareCommandParser;
import com.hazelcast.nio.ascii.SocketTextReader;

import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.STATS;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.VERSION;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.UNKNOWN;

public class SimpleCommandParser extends TypeAwareCommandParser {
    public SimpleCommandParser(TextCommandConstants.TextCommandType type) {
        super(type);
    }

    public TextCommand parser(SocketTextReader socketTextReader, String cmd, int space) {
        if (type == QUIT) {
            return new SimpleCommand(type);
        } else if (type == STATS) {
            return new StatsCommand();
        } else if (type == VERSION) {
            return new VersionCommand(type);
        } else {
            return new ErrorCommand(UNKNOWN);
        }
    }
}

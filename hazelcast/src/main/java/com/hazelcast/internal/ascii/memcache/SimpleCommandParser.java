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

package com.hazelcast.internal.ascii.memcache;

import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType;
import com.hazelcast.internal.ascii.TypeAwareCommandParser;
import com.hazelcast.internal.nio.ascii.TextDecoder;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.STATS;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.UNKNOWN;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.VERSION;

public class SimpleCommandParser extends TypeAwareCommandParser {

    public SimpleCommandParser(TextCommandType type) {
        super(type);
    }

    @Override
    public TextCommand parser(TextDecoder decoder, String cmd, int space) {
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

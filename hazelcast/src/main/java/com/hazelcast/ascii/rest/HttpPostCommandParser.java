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

package com.hazelcast.ascii.rest;

import com.hazelcast.ascii.CommandParser;
import com.hazelcast.ascii.TextCommand;
import com.hazelcast.ascii.memcache.ErrorCommand;
import com.hazelcast.nio.ascii.SocketTextReader;

import java.util.StringTokenizer;

import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;

public class HttpPostCommandParser implements CommandParser {

    public TextCommand parser(SocketTextReader socketTextReader, String cmd, int space) {
        StringTokenizer st = new StringTokenizer(cmd);
        st.nextToken();
        String uri = null;
        if (st.hasMoreTokens()) {
            uri = st.nextToken();
        } else {
            return new ErrorCommand(ERROR_CLIENT);
        }
        return new HttpPostCommand(socketTextReader, uri);
    }
}

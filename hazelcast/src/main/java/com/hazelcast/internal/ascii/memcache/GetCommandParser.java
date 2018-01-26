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

package com.hazelcast.internal.ascii.memcache;

import com.hazelcast.internal.ascii.CommandParser;
import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.nio.ascii.TextChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class GetCommandParser implements CommandParser {

    @Override
    public TextCommand parser(TextChannelInboundHandler readHandler, String cmd, int space) {
        String key = cmd.substring(space + 1);
        if (key.indexOf(' ') == -1) {
            GetCommand r = new GetCommand(key);
            readHandler.publishRequest(r);
        } else {
            StringTokenizer st = new StringTokenizer(key);
            List<String> keys = new ArrayList<String>();
            while (st.hasMoreTokens()) {
                String singleKey = st.nextToken();
                keys.add(singleKey);
            }
            readHandler.publishRequest(new BulkGetCommand(keys));
        }
        return null;
    }
}

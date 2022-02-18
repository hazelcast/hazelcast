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

import com.hazelcast.internal.ascii.CommandParser;
import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.nio.ascii.TextDecoder;

import java.util.StringTokenizer;

public class DeleteCommandParser implements CommandParser {
    @Override
    public TextCommand parser(TextDecoder decoder, String cmd, int space) {
        StringTokenizer st = new StringTokenizer(cmd);
        st.nextToken();
        String key = null;
        int expiration = 0;
        boolean noReply = false;
        if (st.hasMoreTokens()) {
            key = st.nextToken();
        }
        if (st.hasMoreTokens()) {
            expiration = Integer.parseInt(st.nextToken());
        }
        if (st.hasMoreTokens()) {
            noReply = "noreply".equals(st.nextToken());
        }
        return new DeleteCommand(key, expiration, noReply);
    }
}

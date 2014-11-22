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
import com.hazelcast.nio.ascii.TextByteBufferReader;

import java.util.StringTokenizer;

import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;

/**
 * User: sancar
 * Date: 3/8/13
 * Time: 1:52 PM
 */
public class TouchCommandParser extends TypeAwareCommandParser {

    public TouchCommandParser(TextCommandConstants.TextCommandType type) {
        super(type);
    }

    public TextCommand parser(TextByteBufferReader textByteBufferReader, String cmd, int space) {
        StringTokenizer st = new StringTokenizer(cmd);
        st.nextToken();
        String key = null;
        int expiration = 0;
        boolean noReply = false;
        if (st.hasMoreTokens()) {
            key = st.nextToken();
        } else {
            return new ErrorCommand(ERROR_CLIENT);
        }
        if (st.hasMoreTokens()) {
            expiration = Integer.parseInt(st.nextToken());
        } else {
            return new ErrorCommand(ERROR_CLIENT);
        }
        if (st.hasMoreTokens()) {
            noReply = "noreply".equals(st.nextToken());
        }
        return new TouchCommand(type, key, expiration, noReply);
    }
}

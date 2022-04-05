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

import com.hazelcast.internal.ascii.AbstractTextCommand;
import com.hazelcast.internal.ascii.TextCommandConstants;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.util.StringUtil.stringToBytes;

public class VersionCommand extends AbstractTextCommand {

    private static final byte[] VERSION = stringToBytes("VERSION Hazelcast\r\n");

    protected VersionCommand(TextCommandConstants.TextCommandType type) {
        super(type);
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        dst.put(VERSION);
        return true;
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        return true;
    }
}

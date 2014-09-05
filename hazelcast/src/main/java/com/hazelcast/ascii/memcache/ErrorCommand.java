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

import com.hazelcast.ascii.AbstractTextCommand;
import com.hazelcast.ascii.TextCommandConstants;
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;

import static com.hazelcast.util.StringUtil.stringToBytes;

public class ErrorCommand extends AbstractTextCommand {
    ByteBuffer response;
    private final String message;

    public ErrorCommand(TextCommandConstants.TextCommandType type) {
        this(type, null);
    }

    public ErrorCommand(TextCommandConstants.TextCommandType type, String message) {
        super(type);
        byte[] error = TextCommandConstants.ERROR;
        if (type == TextCommandConstants.TextCommandType.ERROR_CLIENT) {
            error = TextCommandConstants.CLIENT_ERROR;
        } else if (type == TextCommandConstants.TextCommandType.ERROR_SERVER) {
            error = TextCommandConstants.SERVER_ERROR;
        }
        this.message = message;
        byte[] msg = (message == null) ? null : stringToBytes(message);
        int total = error.length;
        if (msg != null) {
            total += msg.length;
        }
        total += 2;
        response = ByteBuffer.allocate(total);
        response.put(error);
        if (msg != null) {
            response.put(msg);
        }
        response.put(TextCommandConstants.RETURN);
        response.flip();
    }

    public boolean readFrom(ByteBuffer cb) {
        return true;
    }

    public boolean writeTo(ByteBuffer bb) {
        IOUtil.copyToHeapBuffer(response, bb);
        return !response.hasRemaining();
    }

    @Override
    public String toString() {
        return "ErrorCommand{"
                + "type=" + type
                + ", msg=" + message
                + '}'
                + super.toString();
    }
}

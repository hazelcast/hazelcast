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

import static com.hazelcast.internal.ascii.TextCommandConstants.CLIENT_ERROR;
import static com.hazelcast.internal.ascii.TextCommandConstants.ERROR;
import static com.hazelcast.internal.ascii.TextCommandConstants.SERVER_ERROR;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_SERVER;
import static com.hazelcast.internal.nio.IOUtil.copyToHeapBuffer;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

public class ErrorCommand extends AbstractTextCommand {
    private ByteBuffer response;
    private final String message;

    public ErrorCommand(TextCommandConstants.TextCommandType type) {
        this(type, null);
    }

    public ErrorCommand(TextCommandConstants.TextCommandType type, String message) {
        super(type);
        byte[] error = ERROR;
        if (type == ERROR_CLIENT) {
            error = CLIENT_ERROR;
        } else if (type == ERROR_SERVER) {
            error = SERVER_ERROR;
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
        upcast(response).flip();
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        return true;
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        copyToHeapBuffer(response, dst);
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

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

package com.hazelcast.internal.ascii;

import com.hazelcast.internal.nio.ascii.TextDecoder;
import com.hazelcast.internal.nio.ascii.TextEncoder;

public abstract class AbstractTextCommand implements TextCommand {
    protected final TextCommandConstants.TextCommandType type;
    private TextDecoder decoder;
    private TextEncoder encoder;
    private long requestId = -1;

    protected AbstractTextCommand(TextCommandConstants.TextCommandType type) {
        this.type = type;
    }

    @Override
    public int getFrameLength() {
        return 0;
    }

    @Override
    public TextCommandConstants.TextCommandType getType() {
        return type;
    }

    @Override
    public TextDecoder getDecoder() {
        return decoder;
    }

    @Override
    public TextEncoder getEncoder() {
        return encoder;
    }

    @Override
    public long getRequestId() {
        return requestId;
    }

    @Override
    public void init(TextDecoder decoder, long requestId) {
        this.decoder = decoder;
        this.requestId = requestId;
        this.encoder = decoder.getEncoder();
    }

    @Override
    public boolean isUrgent() {
        return false;
    }

    @Override
    public boolean shouldReply() {
        return true;
    }

    @Override
    public String toString() {
        return "AbstractTextCommand[" + type + "]{"
                + "requestId="
                + requestId
                + '}';
    }
}

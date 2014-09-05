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

package com.hazelcast.ascii;

import java.nio.ByteBuffer;

import static com.hazelcast.util.StringUtil.bytesToString;

public class NoOpCommand extends AbstractTextCommand {
    final ByteBuffer response;

    public NoOpCommand(byte[] response) {
        super(TextCommandConstants.TextCommandType.NO_OP);
        this.response = ByteBuffer.wrap(response);
    }

    public boolean readFrom(ByteBuffer cb) {
        return true;
    }

    public boolean writeTo(ByteBuffer bb) {
        while (bb.hasRemaining() && response.hasRemaining()) {
            bb.put(response.get());
        }
        return !response.hasRemaining();
    }

    @Override
    public String toString() {
        return "NoOpCommand {" + bytesToString(response.array()) + "}";
    }
}

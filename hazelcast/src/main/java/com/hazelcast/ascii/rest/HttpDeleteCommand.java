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

import com.hazelcast.ascii.TextCommandConstants;

import java.nio.ByteBuffer;

public class HttpDeleteCommand extends HttpCommand {
    boolean nextLine;

    public HttpDeleteCommand(String uri) {
        super(TextCommandConstants.TextCommandType.HTTP_DELETE, uri);
    }

    public boolean readFrom(ByteBuffer cb) {
        while (cb.hasRemaining()) {
            char c = (char) cb.get();
            if (c == '\n') {
                if (nextLine) {
                    return true;
                }
                nextLine = true;
            } else if (c != '\r') {
                nextLine = false;
            }
        }
        return false;
    }
}

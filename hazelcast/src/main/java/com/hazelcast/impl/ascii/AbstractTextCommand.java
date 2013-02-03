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

package com.hazelcast.impl.ascii;

import com.hazelcast.nio.ascii.SocketTextReader;
import com.hazelcast.nio.ascii.SocketTextWriter;

public abstract class AbstractTextCommand implements TextCommand {
    private SocketTextReader socketTextReader = null;
    private SocketTextWriter socketTextWriter = null;
    private long requestId = -1;
    protected final TextCommandType type;

    protected AbstractTextCommand(TextCommandType type) {
        this.type = type;
    }

    public TextCommandType getType() {
        return type;
    }

    public SocketTextReader getSocketTextReader() {
        return socketTextReader;
    }

    public SocketTextWriter getSocketTextWriter() {
        return socketTextWriter;
    }

    public long getRequestId() {
        return requestId;
    }

    public void init(SocketTextReader socketTextReader, long requestId) {
        this.socketTextReader = socketTextReader;
        this.requestId = requestId;
        this.socketTextWriter = socketTextReader.getSocketTextWriter();
    }

    public void onEnqueue() {
    }

    public boolean shouldReply() {
        return true;
    }

    @Override
    public String toString() {
        return "AbstractTextCommand[" + type + "]{" +
                "requestId=" + requestId +
                '}';
    }
}

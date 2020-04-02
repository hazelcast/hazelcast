/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.io.flowcontrol.simple;

import java.util.UUID;

/**
 * State of a single stream.
 */
public final class SimpleFlowControlStream {

    private final UUID memberId;
    private long remoteMemory;
    private long localMemory;
    private boolean shouldSend;

    public SimpleFlowControlStream(UUID memberId, long remoteMemory, long localMemory) {
        this.memberId = memberId;

        updateMemory(remoteMemory, localMemory);
    }

    public UUID getMemberId() {
        return memberId;
    }

    public long getRemoteMemory() {
        return remoteMemory;
    }

    public long getLocalMemory() {
        return localMemory;
    }

    public void updateMemory(long remoteMemory, long localMemory) {
        this.remoteMemory = remoteMemory;
        this.localMemory = localMemory;
    }

    public boolean isShouldSend() {
        return shouldSend;
    }

    public void setShouldSend(boolean shouldSend) {
        this.shouldSend = shouldSend;
    }
}

/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.row.RowBatch;

import java.util.UUID;

/**
 * Mailbox batch received from the remote member.
 */
public final class InboundBatch {

    private final RowBatch batch;
    private final long ordinal;
    private final boolean last;
    private final UUID senderId;

    public InboundBatch(RowBatch batch, long ordinal, boolean last, UUID senderId) {
        this.batch = batch;
        this.ordinal = ordinal;
        this.last = last;
        this.senderId = senderId;
    }

    public RowBatch getBatch() {
        return batch;
    }

    public long getOrdinal() {
        return ordinal;
    }

    public boolean isLast() {
        return last;
    }

    public UUID getSenderId() {
        return senderId;
    }
}

/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.physical.hash.HashFunction;
import com.hazelcast.sql.impl.row.Row;

/**
 * Unicast sender.
 */
public class UnicastSendExec extends AbstractSendExec {
    /** Hash function. */
    private final HashFunction hashFunction;

    /** Contains index of partition outboxes. */
    private final int[] partitionOutboxIndexes;

    public UnicastSendExec(
        Exec upstream,
        Outbox[] outboxes,
        HashFunction hashFunction,
        int[] partitionOutboxIndexes
    ) {
        super(upstream, outboxes);

        this.hashFunction = hashFunction;
        this.partitionOutboxIndexes = partitionOutboxIndexes;
    }

    @Override
    protected boolean pushRow(Row row) {
        Outbox outbox = resolveOutbox(row);

        return outbox.onRow(row);
    }

    private Outbox resolveOutbox(Row row) {
        if (outboxes.length == 1) {
            return outboxes[0];
        } else {
            int hash = hashFunction.getHash(row);
            int part = HashUtil.hashToIndex(hash, partitionOutboxIndexes.length);
            int outboxIndex = partitionOutboxIndexes[part];

            return outboxes[outboxIndex];
        }
    }
}

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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.partitioner.RowPartitioner;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Unicast sender.
 */
public class UnicastSendExec extends AbstractMultiwaySendExec {

    private final UnicastOutboxSendQualifier qualifier;

    public UnicastSendExec(
        int id,
        Exec upstream,
        Outbox[] outboxes,
        RowPartitioner rowPartitioner,
        int[] partitionOutboxIndexes,
        InternalSerializationService serializationService
    ) {
        super(id, upstream, outboxes);

        qualifier = new UnicastOutboxSendQualifier(rowPartitioner, partitionOutboxIndexes, serializationService);
    }

    @Override
    protected void setCurrentBatch(RowBatch batch) {
        qualifier.setBatch(batch);
    }

    @Override
    protected OutboxSendQualifier getOutboxQualifier(int outboxIndex) {
        qualifier.setOutboxIndex(outboxIndex);

        return qualifier;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +  "{id=" + getId() + '}';
    }
}

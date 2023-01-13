/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.sql.impl.row.JetSqlJoinRow;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.Arrays;

class IdAssignerProcessor extends AbstractProcessor {
    private int processorIndex;
    private long counter;

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        // Generate partition index that will lead to unique merger.
        // Each IdAssignerProcessor must have exactly one, unique MergerProcessor
        // and vice versa.
        // Usually there will be more partitions than processor instances,
        // but consecutive partition numbers (e.g. if we used globalProcessorIndex)
        // may be assigned to the same processor instance.

        // mimic algorithm in PartitionArrangement.assignPartitionsToProcessors
        // which currently assigns local partitions in round-robin way, i.e.
        // if there are local partitions 0, 1, 2, 6, 9 and localParallelism=2,
        // one processor instance will get partitions 0, 2, 9 and the other 1, 6.
        // It will have the same result in given partition state on given member.
        // It is enough to choose first one for given processor.

        processorIndex = context.processorPartitions()[0];
        if (getLogger().isFinestEnabled()) {
            getLogger().finest("using partition " + processorIndex + " for local index " + context.localProcessorIndex()
                    + " global index " + context.globalProcessorIndex()
                    + ", processorPartitions=" + Arrays.toString(context.processorPartitions()));
        }
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return tryEmit(new JetSqlJoinRow((JetSqlRow) item, processorIndex, counter++, false));
    }
}

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.dag.source;

import com.hazelcast.jet.impl.ringbuffer.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.io.Pair;
import java.io.File;
import java.util.Iterator;

public class DataFileProducer extends AbstractHazelcastProducer<Pair<Integer, String>> {
    private final long end;
    private final long start;

    public DataFileProducer(JobContext jobContext, int partitionId, String name, long start, long end) {
        super(jobContext, name, partitionId, ByReferenceDataTransferringStrategy.INSTANCE);
        this.end = end;
        this.start = start;
    }

    @Override
    public boolean mustRunOnPartitionThread() {
        return false;
    }

    @Override
    protected Iterator<Pair<Integer, String>> newIterator() {
        return new FileIterator(new File(getName()), this.start, this.end);
    }

    @Override
    public void close() {
    }
}

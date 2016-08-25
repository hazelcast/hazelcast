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

import com.hazelcast.jet.container.ContainerContext;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.io.Pair;

import java.io.File;

public class DataFileReader extends AbstractHazelcastReader<Pair<Integer, String>> {
    private final long end;
    private final long start;

    public DataFileReader(ContainerContext containerContext, int partitionId, String name, long start, long end) {
        super(containerContext, name, partitionId, ByReferenceDataTransferringStrategy.INSTANCE);
        this.end = end;
        this.start = start;
    }

    @Override
    public boolean readFromPartitionThread() {
        return false;
    }

    @Override
    protected void onOpen() {
        File file = new File(getName());
        this.iterator = new FileIterator(file, this.start, this.end);
        this.position = ((FileIterator) this.iterator).getLineNumber();
    }

    @Override
    protected void onClose() {
        this.iterator = null;
    }
}

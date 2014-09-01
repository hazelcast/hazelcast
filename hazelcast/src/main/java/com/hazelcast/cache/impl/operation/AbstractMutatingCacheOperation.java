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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;

import java.io.IOException;

/**
 * Base modifying cache operation, this operation publishes COMPLETE events
 */
abstract class AbstractMutatingCacheOperation
        extends AbstractCacheOperation
        implements BackupAwareOperation {

    protected int completionId;

    protected AbstractMutatingCacheOperation() {
    }

    protected AbstractMutatingCacheOperation(String name, Data key, int completionId) {
        super(name, key);
        this.completionId = completionId;
    }

    @Override
    public void afterRun() throws Exception {
        cache.publishCompletedEvent(name,completionId, key, key.hashCode());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeInt(completionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        completionId = in.readInt();
    }

}

/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.processor.BackupAwareEntryProcessor;
import com.hazelcast.collection.processor.Entry;
import com.hazelcast.collection.processor.WaitSupportedEntryProcessor;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * @ali 1/10/13
 */
public class LockEntryProcessor extends MultiMapEntryProcessor<Boolean> implements BackupAwareEntryProcessor, WaitSupportedEntryProcessor {

    public static final long DEFAULT_LOCK_TTL = 5 * 60 * 1000;

    long ttl = DEFAULT_LOCK_TTL;

    long timeout = -1;

    public LockEntryProcessor() {
    }

    public LockEntryProcessor(MultiMapConfig config, long timeout) {
        this.syncBackupCount = config.getSyncBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
        this.timeout = timeout;
    }

    public Boolean execute(Entry entry) {
        if(entry.lock(ttl)){
            shouldBackup = true;
            return true;
        }
        return false;
    }

    public void executeBackup(Entry entry) {
        entry.lock(ttl);
    }

    public boolean shouldWait(Entry entry) {
        return timeout != 0 && !entry.canAcquireLock();
    }

    public long getWaitTimeoutMillis() {
        return timeout;
    }

    public Object onWaitExpire() {
        return false;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(ttl);
        out.writeLong(timeout);
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        ttl = in.readLong();
        timeout = in.readLong();
    }
}

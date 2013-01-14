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
import com.hazelcast.collection.processor.NotifySupportedEntryProcessor;
import com.hazelcast.config.MultiMapConfig;

/**
 * @ali 1/10/13
 */
public class UnlockEntryProcessor extends MultiMapEntryProcessor<Boolean> implements BackupAwareEntryProcessor, NotifySupportedEntryProcessor {

    public UnlockEntryProcessor() {
    }

    public UnlockEntryProcessor(MultiMapConfig config) {
        this.syncBackupCount = config.getSyncBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
    }

    public Boolean execute(Entry entry) {
        if (entry.unlock()){
            shouldBackup = true;
            return true;
        }
        return false;
    }

    public void executeBackup(Entry entry) {
        entry.unlock();
    }

    public boolean shouldNotify(Entry entry) {
        return shouldBackup;
    }
}

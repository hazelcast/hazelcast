/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

public class ClearOperation extends MultiMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    boolean shouldBackup;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    public void beforeRun() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        shouldBackup = container.size() > 0;
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        response = container.clear();
    }

    public void afterRun() throws Exception {
        ((MultiMapService) getService()).getLocalMultiMapStatsImpl(name).incrementOtherOperations();
    }

    public boolean shouldBackup() {
        return shouldBackup;
    }

    public Operation getBackupOperation() {
        return new ClearBackupOperation(name);
    }

    public int getId() {
        return MultiMapDataSerializerHook.CLEAR;
    }

}

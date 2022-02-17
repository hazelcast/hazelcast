/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.util.Collection;

public class RemoveAllOperation extends AbstractBackupAwareMultiMapOperation implements MutatingOperation {

    private Collection<MultiMapRecord> coll;

    public RemoveAllOperation() {
    }

    public RemoveAllOperation(String name, Data dataKey, long threadId) {
        super(name, dataKey, threadId);
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        coll = container.remove(dataKey, executedLocally());
        response = new MultiMapResponse(coll, getValueCollectionType(container));
    }

    @Override
    public void afterRun() throws Exception {
        if (coll != null) {
            getOrCreateContainer().update();
            for (MultiMapRecord record : coll) {
                publishEvent(EntryEventType.REMOVED, dataKey, null, record.getObject());
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return coll != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new RemoveAllBackupOperation(name, dataKey);
    }

    @Override
    public void onWaitExpire() {
        MultiMapContainer container = getOrCreateContainer();
        MultiMapConfig.ValueCollectionType valueCollectionType = getValueCollectionType(container);
        sendResponse(new MultiMapResponse(null, valueCollectionType));
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.REMOVE_ALL;
    }
}

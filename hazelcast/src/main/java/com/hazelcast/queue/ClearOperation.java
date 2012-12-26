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

package com.hazelcast.queue;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.Operation;

import java.util.*;

/**
 * @ali 12/6/12
 */
public class ClearOperation extends QueueBackupAwareOperation {

    private transient List<QueueItem> itemList;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    public void beforeRun() throws Exception {
        itemList = getContainer().itemList();
    }

    public void run() throws Exception {
        QueueContainer container = getContainer();
        container.clear();
        response = true;
        deleteFromStore(false);
    }

    public void afterRun() throws Exception {
        if(deleteFromStore(true)){
            for (QueueItem item: itemList){
                publishEvent(ItemEventType.REMOVED, item.getData());
            }
        }
    }

    private boolean deleteFromStore(boolean async){
        boolean published = false;
        QueueContainer container = getContainer();
        if (container.isStoreAsync() == async && container.getStore().isEnabled()) {
            Set<Long> set = new HashSet<Long>(itemList.size());
            for (QueueItem item: itemList){
                set.add(item.getItemId());
                if (async){
                    published = true;
                    publishEvent(ItemEventType.REMOVED, item.getData());
                }
            }
            container.getStore().deleteAll(set);
        }
        return published;
    }

    public Operation getBackupOperation() {
        return new ClearBackupOperation(name);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }
}

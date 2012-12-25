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

import java.util.List;

/**
 * @ali 12/6/12
 */
public class ClearOperation extends QueueBackupAwareOperation {

    transient List<Data> dataList;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    public void beforeRun() throws Exception {
        if (hasListener()){
            dataList = getContainer().getAsDataList();
        }
    }

    public void run() throws Exception {
        getContainer().clear(false);
        response = true;
    }

    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response) && dataList != null){
            for (Data data: dataList){
                publishEvent(ItemEventType.REMOVED, data);
            }
            dataList.clear();
        }
    }

    public Operation getBackupOperation() {
        return new ClearBackupOperation(name);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }
}

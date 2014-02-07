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

package com.hazelcast.map.operation;

import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.spi.PartitionAwareOperation;

public class LoadKeysOperation extends AbstractMapOperation implements PartitionAwareOperation {


    public LoadKeysOperation(String name) {
        super(name);
    }

    public LoadKeysOperation() {
    }

    public void run() {
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        while (!recordStore.isLoaded()) {
            try {
                System.out.println("Thread.currentThread().getName() = " + Thread.currentThread().getName());
                Thread.sleep(1000);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("recordStore.isLoaded() is true for partitionId:"+getPartitionId());
    }

    @Override
    public Object getResponse() {
        return true;
    }

}

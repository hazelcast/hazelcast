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

import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class WaitInitialLoadOperation extends AbstractOperation {

    MapService mapService;
    String mapName;


    public WaitInitialLoadOperation(String mapName) {
        this.mapName = mapName;
    }

    public WaitInitialLoadOperation() {
    }

    public void run() throws InterruptedException {
        mapService = getService();
        NodeEngine nodeEngine = mapService.getNodeEngine();
        PartitionService partitionService = nodeEngine.getPartitionService();
        List<Integer> partitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
        int count = 0;
        while(!partitions.isEmpty()){
            Iterator<Integer> iterator = partitions.iterator();
            while(iterator.hasNext()) {
                int pid = iterator.next();
                RecordStore recordStore = mapService.getPartitionContainer(pid).getRecordStore(mapName);
                if(recordStore.isLoaded()){
                    iterator.remove();
                }
            }
            Thread.sleep(1000);
            count++;
            if(count % 10 == 0) {
                 nodeEngine.getLogger(this.getClass()).info("Waiting initial load of the map["+ mapName + "] for " + count + " seconds.");
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    public Object getResponse() {
        return true;
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
    }

    @Override
    public String toString() {
        return "WaitInitialLoadOperation{}";
    }

}

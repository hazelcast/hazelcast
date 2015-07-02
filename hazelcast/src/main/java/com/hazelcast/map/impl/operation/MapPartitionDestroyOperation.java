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

package com.hazelcast.map.impl.operation;


import java.io.IOException;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

public class MapPartitionDestroyOperation extends Operation implements PartitionAwareOperation {
    private final PartitionContainer partitionContainer;
    private final String mapName;

    public MapPartitionDestroyOperation(PartitionContainer partitionContainer, String mapName) {
        this.partitionContainer = partitionContainer;
        this.mapName = mapName;
        this.setPartitionId(partitionContainer.getPartitionId());
    }

    @Override
    public void beforeRun() throws Exception {

    }

    @Override
    public void run() throws Exception {
        this.partitionContainer.destroyMap(mapName);
    }

    @Override
    public void afterRun() throws Exception {

    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return partitionContainer.getPartitionId();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        /*
        * It is local only operation and will never be serialized
        * */
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        /*
        * It is local only operation and will never be serialized
        * */
    }
}

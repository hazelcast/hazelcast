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

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * Destroys a backup replicate of a partition map container.
 * It's a backup operation for {@link MapPartitionDestroyOperation}
 *
 */
public class MapPartitionDestroyBackupOperation extends AbstractOperation implements BackupOperation {
    private String mapName;

    public MapPartitionDestroyBackupOperation(String mapName, int partitionId) {
        setPartitionId(partitionId);
        this.mapName = mapName;
    }

    public MapPartitionDestroyBackupOperation() {

    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        PartitionContainer container = mapServiceContext.getPartitionContainer(getPartitionId());
        container.destroyMap(mapName);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
    }
}

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

package com.hazelcast.client;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Collection;

/**
 * User: sancar
 * Date: 8/16/13
 * Time: 11:06 AM
 */
public class DistributedObjectInfos implements Portable {

    private DistributedObjectInfo distributedObjectInfos[];

    DistributedObjectInfos() {

    }

    DistributedObjectInfos(Collection<DistributedObject> distributedObjects) {
        DistributedObjectInfo dos[] = new DistributedObjectInfo[distributedObjects.size()];
        int i = 0;
        for (DistributedObject distributedObject : distributedObjects) {
            dos[i++] = new DistributedObjectInfo(distributedObject.getServiceName(), distributedObject.getId());
        }
        this.distributedObjectInfos = dos;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    public DistributedObjectInfo[] getDistributedObjectInfoArray() {
        return distributedObjectInfos;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.DISTRIBUTED_OBJECTS_INFO;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writePortableArray("dos", distributedObjectInfos);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        final Portable[] portables = reader.readPortableArray("dos");
        distributedObjectInfos = new DistributedObjectInfo[portables.length];
        int i = 0;
        for (Portable portable : portables) {
            distributedObjectInfos[i++] = (DistributedObjectInfo) portable;
        }
    }

}

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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @ali 12/20/12
 */
public class CompareCollectionOperation extends QueueBackupAwareOperation {

    List<Data> dataList;

    transient Map<Long, Data> dataMap;

    boolean retain;

    public CompareCollectionOperation() {
    }

    public CompareCollectionOperation(String name, List<Data> dataList, boolean retain) {
        super(name);
        this.dataList = dataList;
        this.retain = retain;
    }

    public void run() throws Exception {
        response = false;
        dataMap = getContainer().compareCollection(dataList, retain);
        if (dataMap.size() > 0) {
            response = true;
            deleteFromStore(false);
        }
    }

    public void afterRun() throws Exception {
        deleteFromStore(true);
        if (hasListener()) {
            for (Map.Entry<Long, Data> entry : dataMap.entrySet()) {
                Data data = entry.getValue();
                publishEvent(ItemEventType.REMOVED, data);
            }
        }
    }

    private void deleteFromStore(boolean async) {
        QueueContainer container = getContainer();
        if (container.isStoreAsync() == async && container.getStore().isEnabled()) {
            container.getStore().deleteAll(dataMap.keySet());
        }
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new CompareCollectionBackupOperation(name, dataMap.keySet());
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(retain);
        out.writeInt(dataList.size());
        for (Data data : dataList) {
            IOUtil.writeNullableData(out, data);
        }
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        retain = in.readBoolean();
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            dataList.add(IOUtil.readNullableData(in));
        }
    }

}

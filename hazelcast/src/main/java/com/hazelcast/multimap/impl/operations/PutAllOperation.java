/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class PutAllOperation extends AbstractMultiMapOperation implements MutatingOperation{
    private transient int currentIndex;
    private MapEntries mapEntries;
    private long recordId;
    public PutAllOperation() {
        super();
    }

    public PutAllOperation(String name, MapEntries mapEntries) {
        super(name);
        this.mapEntries = mapEntries;
        //NB: general structure copied from c.hz.map.impl.operation.PutAllOperation
        //May benefit from refactoring or default interfaces
    }

    @Override
    public final void run() {
        try {
            runInternal();
        } catch (NativeOutOfMemoryError e) {
            //TODO: implement rerun?
            e.printStackTrace();
        }
    }

    protected void runInternal() {
        //FIXME: potentially bad idiom?
        int size = mapEntries!=null ? mapEntries.size():0;
        while (currentIndex < size) {
            Data dataKey = mapEntries.getKey(currentIndex);
            Data value = mapEntries.getValue(currentIndex);
            put(dataKey, value);
            currentIndex++;
        }
        //FIXME: is this the best place to set response?
        response = true;
    }

    protected void put(Data dataKey, Data dataValue) {
        MultiMapContainer container = getOrCreateContainer();
        Collection c = (Collection)toObject(dataValue);
        //FIXME:do we even need this null check? Maybe use an Optional?
        if(c!=null) {
            Collection<MultiMapRecord> coll = container.getOrCreateMultiMapValue(dataKey).getCollection(false);
            Iterator it = c.iterator();
            while(it.hasNext()){
                Object o = it.next();
                recordId = container.nextId();
                MultiMapRecord record = new MultiMapRecord(recordId, o);
                coll.add(record);
                //NB: we could make a coll.addAll(c) instead if we changed the getObjectCollection impl
                getOrCreateContainer().update();
                //FIXME: there should be a flag to turn this off?
                publishEvent(EntryEventType.ADDED, dataKey, o, null);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = in.readObject();
    }
    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.PUT_ALL;
    }


}

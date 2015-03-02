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

package com.hazelcast.map.impl.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

/**
 * User: dsukhoroslov
 * Date: 28.02.2015
 */
public class TxnMapRequestWithDataMap extends AbstractTxnMapRequest {

    private Map<Data, Data> dataMap = new HashMap<Data, Data>();

    public TxnMapRequestWithDataMap() {
    }

    public TxnMapRequestWithDataMap(String name, TxnMapRequestType requestType, Map<Data, Data> dataMap) {
        super(name, requestType, null, null, null);
        if (dataMap != null) {
        	this.dataMap.putAll(dataMap);
        }
    }

    protected Predicate getPredicate() {
       return null;
    }

    public int getClassId() {
        return MapPortableHook.TXN_REQUEST_WITH_DATAMAP;
    }

    @Override
    protected Object innerCallInternal(final TransactionalMap map) {
        map.putAll(dataMap);
        return null;
    }
    
    protected void readDataInner(ObjectDataInput in) throws IOException {
    	int size = in.readInt();
    	for (int i=0; i<size; i++) {
    		dataMap.put(in.readData(), in.readData());
    	}
    }

    protected void writeDataInner(ObjectDataOutput out) throws IOException {
    	out.writeInt(dataMap.size());
    	for (Map.Entry<Data, Data> entry: dataMap.entrySet()) {
    		out.writeData(entry.getKey());
    		out.writeData(entry.getValue());
    	}
    }


}

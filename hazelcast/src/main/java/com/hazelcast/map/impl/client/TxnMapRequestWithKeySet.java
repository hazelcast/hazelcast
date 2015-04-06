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
import java.util.HashSet;
import java.util.Set;

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
public class TxnMapRequestWithKeySet extends AbstractTxnMapRequest {

    private Set<Data> keySet = new HashSet<Data>();

    public TxnMapRequestWithKeySet() {
    }

    public TxnMapRequestWithKeySet(String name, TxnMapRequestType requestType, Set<Data> keySet) {
        super(name, requestType, null, null, null);
        if (keySet != null) {
        	this.keySet.addAll(keySet);
        }
    }

    protected Predicate getPredicate() {
       return null;
    }

    public int getClassId() {
        return MapPortableHook.TXN_REQUEST_WITH_KEYSET;
    }

    @Override
    protected Object innerCallInternal(final TransactionalMap map) {
        return map.getAll(keySet);
    }
    
    protected void readDataInner(ObjectDataInput in) throws IOException {
    	int size = in.readInt();
    	for (int i=0; i<size; i++) {
    		keySet.add(in.readData());
    	}
    }

    protected void writeDataInner(ObjectDataOutput out) throws IOException {
    	out.writeInt(keySet.size());
    	for (Data key: keySet) {
    		out.writeData(key);
    	}
    }

}

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

package com.hazelcast.map.impl.client;


import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author ali 6/10/13
 */

public class TxnMapRequest extends AbstractTxnMapRequest {

    Predicate predicate;

    public TxnMapRequest() {
    }

    public TxnMapRequest(String name, TxnMapRequestType requestType) {
        super(name, requestType);
    }

    public TxnMapRequest(String name, TxnMapRequestType requestType, Data key) {
        this(name, requestType);
        this.key = key;
    }

    public TxnMapRequest(String name, TxnMapRequestType requestType, Data key, Data value) {
        this(name, requestType, key);
        this.value = value;
    }

    public TxnMapRequest(String name, TxnMapRequestType requestType, Data key, Data value, long ttl, TimeUnit timeUnit) {
        super(name, requestType, key, value, ttl, timeUnit);
    }

    public TxnMapRequest(String name, TxnMapRequestType requestType, Data key, Data value, Data newValue) {
        this(name, requestType, key, value);
        this.newValue = newValue;
    }

    public TxnMapRequest(String name, TxnMapRequestType requestType, Predicate predicate) {
        this(name, requestType, null, null, null);
        this.predicate = predicate;
    }

    public int getClassId() {
        return MapPortableHook.TXN_REQUEST;
    }

    protected Predicate getPredicate() {
        return predicate;
    }

    protected void writeDataInner(ObjectDataOutput writer) throws IOException {
        writer.writeObject(predicate);
    }

    protected void readDataInner(ObjectDataInput reader) throws IOException {
        predicate = reader.readObject();
    }
}

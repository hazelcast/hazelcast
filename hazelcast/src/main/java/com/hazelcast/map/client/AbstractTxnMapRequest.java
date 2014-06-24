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

package com.hazelcast.map.client;

import com.hazelcast.client.SecureRequest;
import com.hazelcast.client.txn.BaseTransactionRequest;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapValueCollection;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;
import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * User: sancar
 * Date: 9/18/13
 * Time: 2:28 PM
 */
public abstract class AbstractTxnMapRequest extends BaseTransactionRequest {

    String name;
    TxnMapRequestType requestType;
    Data key;
    Data value;
    Data newValue;
    long ttl = -1;

    public AbstractTxnMapRequest() {
    }

    public AbstractTxnMapRequest(String name, TxnMapRequestType requestType) {
        this.name = name;
        this.requestType = requestType;
    }

    public AbstractTxnMapRequest(String name, TxnMapRequestType requestType, Data key) {
        this(name, requestType);
        this.key = key;
    }

    public AbstractTxnMapRequest(String name, TxnMapRequestType requestType, Data key, Data value) {
        this(name, requestType, key);
        this.value = value;
    }

    public AbstractTxnMapRequest(String name, TxnMapRequestType requestType, Data key, Data value, Data newValue) {
        this(name, requestType, key, value);
        this.newValue = newValue;
    }

    public AbstractTxnMapRequest(String name, TxnMapRequestType requestType, Data key, Data value, long ttl, TimeUnit timeUnit) {
        this(name, requestType, key, value);
        this.ttl = timeUnit == null ? ttl : timeUnit.toMillis(ttl);
    }


    public Object innerCall() throws Exception {
        final TransactionContext context = getEndpoint().getTransactionContext(txnId);
        final TransactionalMap map = context.getMap(name);
        switch (requestType) {
            case CONTAINS_KEY:
                return map.containsKey(key);
            case GET:
                return map.get(key);
            case GET_FOR_UPDATE:
                return map.getForUpdate(key);
            case SIZE:
                return map.size();
            case PUT:
                return map.put(key, value);
            case PUT_WITH_TTL:
                return map.put(key, value, ttl, TimeUnit.MILLISECONDS);
            case PUT_IF_ABSENT:
                return map.putIfAbsent(key, value);
            case REPLACE:
                return map.replace(key, value);
            case REPLACE_IF_SAME:
                return map.replace(key, value, newValue);
            case SET:
                map.set(key, value);
                break;
            case REMOVE:
                return map.remove(key);
            case DELETE:
                map.delete(key);
                break;
            case REMOVE_IF_SAME:
                return map.remove(key, value);
            case KEYSET:
                return getMapKeySet(map.keySet());
            case KEYSET_BY_PREDICATE:
                return getMapKeySet(map.keySet(getPredicate()));
            case VALUES:
                return getMapValueCollection(map.values());
            case VALUES_BY_PREDICATE:
                return getMapValueCollection(map.values(getPredicate()));

        }
        return null;
    }

    private MapKeySet getMapKeySet(Set keySet) {
        final HashSet<Data> dataKeySet = new HashSet<Data>();
        for (Object key : keySet) {
            final Data dataKey = serializationService.toData(key);
            dataKeySet.add(dataKey);
        }
        return new MapKeySet(dataKeySet);
    }

    private MapValueCollection getMapValueCollection(Collection coll) {
        final HashSet<Data> valuesCollection = new HashSet<Data>(coll.size());
        for (Object value : coll) {
            final Data dataValue = serializationService.toData(value);
            valuesCollection.add(dataValue);
        }
        return new MapValueCollection(valuesCollection);
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        writer.writeInt("t", requestType.type);
        final ObjectDataOutput out = writer.getRawDataOutput();
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
        IOUtil.writeNullableData(out, newValue);
        writeDataInner(out);
        out.writeLong(ttl);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        requestType = TxnMapRequestType.getByType(reader.readInt("t"));
        final ObjectDataInput in = reader.getRawDataInput();
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
        newValue = IOUtil.readNullableData(in);
        readDataInner(in);
        ttl = in.readLong();
    }

    protected abstract Predicate getPredicate();

    protected abstract void writeDataInner(ObjectDataOutput writer) throws IOException;

    protected abstract void readDataInner(ObjectDataInput reader) throws IOException;

    public enum TxnMapRequestType {
        CONTAINS_KEY(1),
        GET(2),
        SIZE(3),
        PUT(4),
        PUT_IF_ABSENT(5),
        REPLACE(6),
        REPLACE_IF_SAME(7),
        SET(8),
        REMOVE(9),
        DELETE(10),
        REMOVE_IF_SAME(11),
        KEYSET(12),
        KEYSET_BY_PREDICATE(13),
        VALUES(14),
        VALUES_BY_PREDICATE(15),
        GET_FOR_UPDATE(16),
        PUT_WITH_TTL(17);
        int type;

        TxnMapRequestType(int i) {
            type = i;
        }

        public static TxnMapRequestType getByType(int type) {
            for (TxnMapRequestType requestType : values()) {
                if (requestType.type == type) {
                    return requestType;
                }
            }
            return null;
        }
    }

    public Permission getRequiredPermission() {
        String action;
        boolean isLock = true;
        switch (requestType) {
            case CONTAINS_KEY:
            case GET:
            case SIZE:
            case KEYSET:
            case KEYSET_BY_PREDICATE:
            case VALUES:
            case VALUES_BY_PREDICATE:
                action = ActionConstants.ACTION_READ;
                isLock = false;
                break;
            case GET_FOR_UPDATE:
                action = ActionConstants.ACTION_READ;
                break;
            case PUT:
            case PUT_IF_ABSENT:
            case REPLACE:
            case REPLACE_IF_SAME:
            case SET:
            case PUT_WITH_TTL:
                action = ActionConstants.ACTION_PUT;
                break;
            case REMOVE:
            case DELETE:
            case REMOVE_IF_SAME:
                action = ActionConstants.ACTION_REMOVE;
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + requestType);
        }
        if (isLock) {
            return new MapPermission(name, action, ActionConstants.ACTION_LOCK);
        }
        return new MapPermission(name, action);
    }
}

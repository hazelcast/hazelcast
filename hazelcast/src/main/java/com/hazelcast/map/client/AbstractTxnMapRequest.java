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
        return innerCallInternal(map);
    }


    private Object innerCallInternal(final TransactionalMap map) {
        Object result = null;
        switch (requestType) {
            case CONTAINS_KEY:
                result = map.containsKey(key);
                break;
            case GET:
                result = map.get(key);
                break;
            case GET_FOR_UPDATE:
                result = map.getForUpdate(key);
                break;
            case SIZE:
                result = map.size();
                break;
            case PUT:
                result = map.put(key, value);
                break;
            case PUT_WITH_TTL:
                result = map.put(key, value, ttl, TimeUnit.MILLISECONDS);
                break;
            case PUT_IF_ABSENT:
                result = map.putIfAbsent(key, value);
                break;
            case REPLACE:
                result = map.replace(key, value);
                break;
            case REPLACE_IF_SAME:
                result = map.replace(key, value, newValue);
                break;
            case SET:
                map.set(key, value);
                break;
            case REMOVE:
                result = map.remove(key);
                break;
            case DELETE:
                map.delete(key);
                break;
            case REMOVE_IF_SAME:
                result = map.remove(key, value);
                break;
            case KEYSET:
                result = getMapKeySet(map.keySet());
                break;
            case KEYSET_BY_PREDICATE:
                result = getMapKeySet(map.keySet(getPredicate()));
                break;
            case VALUES:
                result = getMapValueCollection(map.values());
                break;
            case VALUES_BY_PREDICATE:
                result = getMapValueCollection(map.values(getPredicate()));
                break;
            default:
                throw new IllegalArgumentException("Not a known TxnMapRequestType [" + requestType + "]");
        }
        return result;
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
        CONTAINS_KEY(1) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapReadPermission(name);
            }
        },

        GET(2) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapReadPermission(name);
            }
        },
        SIZE(3) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapReadPermission(name);
            }
        },
        PUT(4) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedWritePermission(name);
            }
        },
        PUT_IF_ABSENT(5) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedWritePermission(name);
            }
        },
        REPLACE(6) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedWritePermission(name);
            }
        },
        REPLACE_IF_SAME(7) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedWritePermission(name);
            }
        },
        SET(8) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedWritePermission(name);
            }
        },
        REMOVE(9) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedRemovePermission(name);
            }
        },
        DELETE(10) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedRemovePermission(name);
            }
        },
        REMOVE_IF_SAME(11) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedRemovePermission(name);
            }
        },
        KEYSET(12) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapReadPermission(name);
            }
        },
        KEYSET_BY_PREDICATE(13) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapReadPermission(name);
            }
        },
        VALUES(14) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapReadPermission(name);
            }
        },
        VALUES_BY_PREDICATE(15) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapReadPermission(name);
            }
        },
        GET_FOR_UPDATE(16) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedReadPermission(name);
            }
        },
        PUT_WITH_TTL(17) {
            @Override
            Permission getRequiredPermission(String name) {
                return getMapLockedWritePermission(name);
            }
        };
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

        abstract Permission getRequiredPermission(String name);

        private static Permission getMapReadPermission(String name) {
            return new MapPermission(name, ActionConstants.ACTION_READ);
        }

        private static Permission getMapLockedReadPermission(String name) {
            return new MapPermission(name, ActionConstants.ACTION_READ, ActionConstants.ACTION_LOCK);
        }

        private static Permission getMapLockedWritePermission(String name) {
            return new MapPermission(name, ActionConstants.ACTION_PUT, ActionConstants.ACTION_LOCK);
        }

        private static Permission getMapLockedRemovePermission(String name) {
            return new MapPermission(name, ActionConstants.ACTION_REMOVE, ActionConstants.ACTION_LOCK);
        }
    }

    public Permission getRequiredPermission() {
        return requestType.getRequiredPermission(name);
    }
}

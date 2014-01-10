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

package com.hazelcast.multimap.operations.client;

import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 * @author ali 6/10/13
 */
public class TxnMultiMapGetRequest extends TxnMultiMapRequest {

    Data key;

    public TxnMultiMapGetRequest() {
    }

    public TxnMultiMapGetRequest(String name, Data key) {
        super(name);
        this.key = key;
    }

    public Object call() throws Exception {

        final TransactionContext context = getEndpoint().getTransactionContext(txnId);
        final Collection<Object> objects = context.getMultiMap(name).get(key);
        Collection<Data> coll = createCollection(objects.size());
        for (Object object : objects) {
            final Data data = toData(object);
            coll.add(data);
        }
        return new PortableCollection(coll);
    }

    public int getClassId() {
        return MultiMapPortableHook.TXN_MM_GET;
    }

    private Collection<Data> createCollection(int size){
        final MultiMapConfig config = getClientEngine().getConfig().findMultiMapConfig(name);
        if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)){
            return new HashSet<Data>(size);
        }
        else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST)){
            return new ArrayList<Data>(size);
        }
        return null;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        key.writeData(writer.getRawDataOutput());
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        key = new Data();
        key.readData(reader.getRawDataInput());
    }
}

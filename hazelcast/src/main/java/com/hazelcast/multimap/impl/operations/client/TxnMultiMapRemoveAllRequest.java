package com.hazelcast.multimap.impl.operations.client;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.transaction.TransactionContext;
import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

public class TxnMultiMapRemoveAllRequest extends TxnMultiMapRequest {

    Data key;

    public TxnMultiMapRemoveAllRequest() {
    }

    public TxnMultiMapRemoveAllRequest(String name, Data key) {
        super(name);
        this.key = key;
    }

    public Object innerCall() throws Exception {
        final TransactionContext context = getEndpoint().getTransactionContext(txnId);
        final TransactionalMultiMap<Object, Object> multiMap = context.getMultiMap(name);
        final Collection<Object> objects = multiMap.remove(key);
        Collection<Data> coll = createCollection(objects.size());
        for (Object object : objects) {
            final Data data = toData(object);
            coll.add(data);
        }
        return new PortableCollection(coll);
    }

    public int getClassId() {
        return MultiMapPortableHook.TXN_MM_REMOVEALL;
    }

    private Collection<Data> createCollection(int size) {
        final MultiMapConfig config = getClientEngine().getConfig().findMultiMapConfig(name);
        if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)) {
            return new HashSet<Data>(size);
        } else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.LIST)) {
            return new ArrayList<Data>(size);
        }
        return null;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_REMOVE);
    }
}

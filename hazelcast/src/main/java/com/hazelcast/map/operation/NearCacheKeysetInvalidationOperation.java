package com.hazelcast.map.operation;

import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * User: ahmetmircik
 * Date: 10/31/13
 */
public class NearCacheKeySetInvalidationOperation extends AbstractOperation {
    MapService mapService;
    Set<Data> keys;
    String mapName;


    public NearCacheKeySetInvalidationOperation(String mapName, Set<Data> keys) {
        this.keys = keys;
        this.mapName = mapName;
    }

    public NearCacheKeySetInvalidationOperation() {
    }

    public void run() {
        mapService = getService();
        mapService.invalidateNearCache(mapName, keys);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        final int size = in.readInt();
        if (size > -1) {
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                keys.add(data);
            }
        }
    }

    @Override
    public String toString() {
        return "InvalidateNearCacheOperation{}";
    }

    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}

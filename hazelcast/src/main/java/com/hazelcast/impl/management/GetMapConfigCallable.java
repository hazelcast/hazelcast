package com.hazelcast.impl.management;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.Processable;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public class GetMapConfigCallable extends ClusterServiceCallable implements Callable<MapConfig>, HazelcastInstanceAware, DataSerializable {

    private static final long serialVersionUID = -3496139512682608428L;
    protected String mapName;

    public GetMapConfigCallable() {
    }

    public GetMapConfigCallable(String mapName) {
        super();
        this.mapName = mapName;
    }

    public MapConfig call() throws Exception {
        final AtomicReference<MapConfig> ref = new AtomicReference<MapConfig>();
        getClusterService().enqueueAndWait(new Processable() {
            public void process() {
                final CMap cmap = getCMap(mapName);
                MapConfig cfg = cmap.getRuntimeConfig();
                ref.set(cfg);
            }
        }, 5);
        return ref.get();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
    }
}

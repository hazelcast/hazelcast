package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.config.MapConfig;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.Processable;

public class UpdateMapConfigCallable extends GetMapConfigCallable {
	
	private static final long serialVersionUID = -7634684790969633350L;
	
	private MapConfig mapConfig;
	
    public UpdateMapConfigCallable() {
    }
    
    public UpdateMapConfigCallable(String mapName, MapConfig mapConfig) {
		super(mapName);
		this.mapConfig = mapConfig;
	}

	public MapConfig call() throws Exception {
        getClusterService().enqueueAndReturn(new Processable() {
			public void process() {
				final CMap cmap = getCMap();
				cmap.setRuntimeConfig(mapConfig);
			}
		});	
        return null;
    }

    public void writeData(DataOutput out) throws IOException {
    	super.writeData(out);
		mapConfig.writeData(out);
	}

	public void readData(DataInput in) throws IOException {
		super.readData(in);
		mapConfig = new MapConfig();
		mapConfig.readData(in);
	}
}

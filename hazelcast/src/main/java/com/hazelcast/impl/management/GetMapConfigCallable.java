/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.management;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

//import com.hazelcast.impl.CMap;

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
//        getClusterService().enqueueAndWait(new Processable() {
//            public void process() {
//                final CMap cmap = getCMap(mapName);
//                MapConfig cfg = cmap.getRuntimeConfig();
//                ref.set(cfg);
//            }
//        }, 5);
        return ref.get();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
    }
}

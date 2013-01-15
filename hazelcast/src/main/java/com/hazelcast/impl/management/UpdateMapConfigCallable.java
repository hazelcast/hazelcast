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

package com.hazelcast.impl.management;

import com.hazelcast.config.MapConfig;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.Processable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
                final CMap cmap = getCMap(mapName);
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

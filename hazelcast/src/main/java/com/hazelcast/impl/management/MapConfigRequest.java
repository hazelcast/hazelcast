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
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapConfigRequest implements ConsoleRequest {

    private String map;
    private MapConfig config;
    private boolean update;
    private Address target;

    public MapConfigRequest() {
    }

    public MapConfigRequest(String map, MapConfig config) {
        super();
        this.map = map;
        this.config = config;
        this.update = true;
    }

    public MapConfigRequest(String map, Address target) {
        super();
        this.map = map;
        this.target = target;
        this.update = false;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MAP_CONFIG;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos)
            throws Exception {
        if (update) {
            mcs.callOnAllMembers(new UpdateMapConfigCallable(map, config));
        } else {
            MapConfig cfg = (MapConfig) mcs.call(target, new GetMapConfigCallable(map));
            if (cfg != null) {
                dos.writeBoolean(true);
                cfg.writeData(dos);
            } else {
                dos.writeBoolean(false);
            }
        }
    }

    public Object readResponse(DataInput in) throws IOException {
        if (!update) {
            if (in.readBoolean()) {
                MapConfig cfg = new MapConfig();
                cfg.readData(in);
                return cfg;
            }
        }
        return null;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(map);
        out.writeBoolean(update);
        if (update) {
            config.writeData(out);
        } else {
            target.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        map = in.readUTF();
        update = in.readBoolean();
        if (update) {
            config = new MapConfig();
            config.readData(in);
        } else {
            target = new Address();
            target.readData(in);
        }
    }
}

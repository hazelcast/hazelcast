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

package com.hazelcast.management.request;

import com.hazelcast.config.MapConfig;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.management.operation.GetMapConfigOperation;
import com.hazelcast.management.operation.UpdateMapConfigOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

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

    public Address getTarget() {
        return target;
    }

    public void setTarget(Address target) {
        this.target = target;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MAP_CONFIG;
    }

    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos)
            throws Exception {
        dos.writeBoolean(update);
        if (update) {
            mcs.callOnAllMembers(new UpdateMapConfigOperation(map, config));
            dos.writeUTF("success");
        } else {
            MapConfig cfg = (MapConfig) mcs.call(target, new GetMapConfigOperation(map));
            if (cfg != null) {
                dos.writeBoolean(true);
                cfg.writeData(dos);
            } else {
                dos.writeBoolean(false);
            }
        }
    }

    public Object readResponse(ObjectDataInput in) throws IOException {
        update = in.readBoolean();

        if (!update) {
            if (in.readBoolean()) {
                MapConfig cfg = new MapConfig();
                cfg.readData(in);
                return cfg;
            } else {
                return null;
            }
        }
        return in.readUTF();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(map);
        out.writeBoolean(update);
        if (update) {
            config.writeData(out);
        } else {
            target.writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
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

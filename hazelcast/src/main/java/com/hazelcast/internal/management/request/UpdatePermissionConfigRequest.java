/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.request;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.management.dto.PermissionConfigDTO;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;

import java.util.HashSet;
import java.util.Set;

/**
 * Helper class to serialize/deserialize client permission update requests.
 *
 * @see com.hazelcast.internal.ascii.rest.HttpCommandProcessor
 */
public class UpdatePermissionConfigRequest implements JsonSerializable {

    private Set<PermissionConfig> permissionConfigs;

    public UpdatePermissionConfigRequest() {

    }

    public UpdatePermissionConfigRequest(Set<PermissionConfig> permissionConfigs) {
        this.permissionConfigs = permissionConfigs;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        JsonArray permissionConfigsArr = new JsonArray();
        for (PermissionConfig permissionConfig : permissionConfigs) {
            permissionConfigsArr.add(new PermissionConfigDTO(permissionConfig).toJson());
        }
        root.add("permissionConfigs", permissionConfigsArr);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        JsonArray confArray = json.get("permissionConfigs").asArray();
        permissionConfigs = new HashSet<PermissionConfig>(confArray.size());
        for (JsonValue permissionConfigDTO : confArray.values()) {
            PermissionConfigDTO dto = new PermissionConfigDTO();
            dto.fromJson(permissionConfigDTO.asObject());
            permissionConfigs.add(dto.getPermissionConfig());
        }
    }

    public Set<PermissionConfig> getPermissionConfigs() {
        return permissionConfigs;
    }
}

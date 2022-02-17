/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.HashSet;
import java.util.Set;

/**
 * DTO object that provides serialization/deserialization support
 * for {@link PermissionConfig}
 */
public class PermissionConfigDTO implements JsonSerializable {

    private PermissionConfig permissionConfig;

    public PermissionConfigDTO() {
    }

    public PermissionConfigDTO(PermissionConfig permissionConfig) {
        this.permissionConfig = permissionConfig;
    }

    @Override
    public JsonObject toJson() {
        JsonObject object = new JsonObject();
        object.add("permissionType", permissionConfig.getType().getNodeName());
        object.add("name", permissionConfig.getName());
        if (StringUtil.isNullOrEmptyAfterTrim(permissionConfig.getPrincipal())) {
            object.add("principal", "*");
        } else {
            object.add("principal", permissionConfig.getPrincipal());
        }

        Set<String> endpoints = permissionConfig.getEndpoints();
        if (endpoints != null) {
            JsonArray endpointsArray = new JsonArray();
            for (String endpoint : endpoints) {
                endpointsArray.add(endpoint);
            }
            object.add("endpoints", endpointsArray);
        }

        Set<String> actions = permissionConfig.getActions();
        if (actions != null) {
            JsonArray actionsArray = new JsonArray();
            for (String action : actions) {
                actionsArray.add(action);
            }
            object.add("actions", actionsArray);
        }

        return object;
    }

    @Override
    public void fromJson(JsonObject json) {
        permissionConfig = new PermissionConfig();
        permissionConfig.setType(PermissionConfig.PermissionType.getType(json.getString("permissionType", null)));
        permissionConfig.setName(json.get("name").asString());
        permissionConfig.setPrincipal(json.getString("principal", "*"));

        JsonValue endpointsVal = json.get("endpoints");
        if (endpointsVal != null) {
            Set<String> endpoints = new HashSet<>();
            for (JsonValue endpoint : endpointsVal.asArray().values()) {
                endpoints.add(endpoint.asString());
            }
            permissionConfig.setEndpoints(endpoints);
        }

        JsonValue actionsVal = json.get("actions");
        if (actionsVal != null) {
            Set<String> actions = new HashSet<>();
            for (JsonValue action : actionsVal.asArray().values()) {
                actions.add(action.asString());
            }
            permissionConfig.setActions(actions);
        }

    }

    public PermissionConfig getPermissionConfig() {
        return permissionConfig;
    }

}

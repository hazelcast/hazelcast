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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.json.JsonSerializable;

import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getString;

class MergePolicyConfigDTO implements JsonSerializable {

    private MergePolicyConfig mergePolicyConfig;

    MergePolicyConfigDTO() {
    }

    MergePolicyConfigDTO(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = mergePolicyConfig;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("policy", mergePolicyConfig.getPolicy());
        root.add("batchSize", mergePolicyConfig.getBatchSize());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        mergePolicyConfig = new MergePolicyConfig();
        mergePolicyConfig.setPolicy(getString(json, "policy"));
        mergePolicyConfig.setBatchSize(getInt(json, "batchSize"));
    }

    public MergePolicyConfig getConfig() {
        return mergePolicyConfig;
    }
}

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

import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.proxyservice.ProxyService;

import static com.hazelcast.internal.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.internal.jmx.ProxyServiceMBean}.
 */
public class ProxyServiceDTO implements JsonSerializable {

    public int proxyCount;

    public ProxyServiceDTO() {
    }

    public ProxyServiceDTO(ProxyService proxyService) {
        this.proxyCount = proxyService.getProxyCount();
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("proxyCount", proxyCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        proxyCount = getInt(json, "proxyCount", -1);
    }
}

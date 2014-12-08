/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.spi.ProxyService;

import static com.hazelcast.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.ProxyServiceMBean}.
 */
public class SerializableProxyServiceBean implements JsonSerializable {

    private int proxyCount;

    public SerializableProxyServiceBean() {
    }

    public SerializableProxyServiceBean(ProxyService proxyService) {
        this.proxyCount = proxyService.getProxyCount();
    }

    public int getProxyCount() {
        return proxyCount;
    }

    public void setProxyCount(int proxyCount) {
        this.proxyCount = proxyCount;
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

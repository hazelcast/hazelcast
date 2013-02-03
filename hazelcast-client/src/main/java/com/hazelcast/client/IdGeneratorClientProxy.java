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

package com.hazelcast.client;

import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.ClusterOperation;

public class IdGeneratorClientProxy implements IdGenerator {
    private final String name;
    private final ProxyHelper proxyHelper;

    public IdGeneratorClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public String getName() {
        return name.substring(Prefix.IDGEN.length());
    }

    public long newId() {
        return (Long) proxyHelper.doOp(ClusterOperation.NEW_ID, null, null);
    }

    public InstanceType getInstanceType() {
        return InstanceType.ID_GENERATOR;
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }
}

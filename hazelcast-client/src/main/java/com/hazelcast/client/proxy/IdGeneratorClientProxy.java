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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.ProxyHelper;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.nio.protocol.Command;

public class IdGeneratorClientProxy implements IdGenerator {
    private final String name;
    private final ProxyHelper proxyHelper;

    public IdGeneratorClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(hazelcastClient.getSerializationService(), hazelcastClient.getConnectionPool());
    }

    public String getName() {
        return name;
    }

    public boolean init(long id) {
        return proxyHelper.doCommandAsBoolean(null, Command.INITID, new String[]{getName(), String.valueOf(id)}, null);
    }

    public long newId() {
        return proxyHelper.doCommandAsInt(null, Command.NEWID, new String[]{getName()}, null);
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }
}

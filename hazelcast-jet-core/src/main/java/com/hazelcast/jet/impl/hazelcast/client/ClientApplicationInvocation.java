/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.hazelcast.client;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.jet.impl.application.AbstractApplicationInvocation;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.nio.Address;

public class ClientApplicationInvocation<T> extends AbstractApplicationInvocation<ClientMessage, T> {
    private final HazelcastClientInstanceImpl client;

    public ClientApplicationInvocation(ClientMessage operation,
                                       Address address,
                                       HazelcastClientInstanceImpl clientInstance) {
        super(operation, address);
        this.client = clientInstance;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T execute(ClientMessage operation, Address address) {
        try {
            return (T) new ClientInvocation(this.client, operation, address).invoke().get();
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }
    }
}

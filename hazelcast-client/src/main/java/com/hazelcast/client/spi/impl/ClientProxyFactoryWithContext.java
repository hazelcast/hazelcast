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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;

/**
 * Factory class for creating client proxies with a {@link ClientContext}.
 * <p>
 * This class creates a client proxy and passes a {@link ClientContext} directly to the constructor.
 */
public abstract class ClientProxyFactoryWithContext implements ClientProxyFactory {

    @Override
    public final ClientProxy create(String id) {
        throw new UnsupportedOperationException("Please use create(String id, ClientContext context) instead!");
    }

    /**
     * Creates a new client proxy with the given ID.
     *
     * @param id      the ID of the client proxy
     * @param context the {@link ClientContext} of the client proxy
     * @return the client proxy
     */
    public abstract ClientProxy create(String id, ClientContext context);
}

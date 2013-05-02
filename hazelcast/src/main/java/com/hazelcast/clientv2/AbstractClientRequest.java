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

package com.hazelcast.clientv2;

import com.hazelcast.nio.Connection;

/**
 * @mdogan 4/29/13
 */
public abstract class AbstractClientRequest implements ClientRequest {

    protected ClientEngine clientEngine;

    protected Object service;

    protected Connection connection;

    public final void setClientEngine(ClientEngine clientEngine) {
        this.clientEngine = clientEngine;
    }

    public final void setService(Object service) {
        this.service = service;
    }

    public final void setConnection(Connection connection) {
        this.connection = connection;
    }
}

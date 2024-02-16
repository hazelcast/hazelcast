/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.dataconnection.HazelcastDataConnection;
import com.hazelcast.jet.core.Processor.Context;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;

/**
 * Base class for context object in {@link SourceBuilder} or {@link SinkBuilder}.
 * The class provides common infrastructure to handle either a
 * {@link com.hazelcast.dataconnection.HazelcastDataConnection} or {@link com.hazelcast.client.config.ClientConfig}
 */
class HazelcastClientBaseContext {

    protected HazelcastClientProxy client;

    HazelcastClientBaseContext(String dataConnectionName, String clientXml, Context context) {
        if (dataConnectionName == null && clientXml == null) {
            throw new IllegalArgumentException(
                    "Either dataConnectionName or clientConfig must be provided. Both are null"
            );
        }

        if (dataConnectionName != null) {
            HazelcastDataConnection dataConnection =
                    context.dataConnectionService()
                           .getAndRetainDataConnection(dataConnectionName, HazelcastDataConnection.class);
            try {
                this.client = (HazelcastClientProxy) dataConnection.getClient();
            } finally {
                dataConnection.release();
            }
        } else {
            this.client = (HazelcastClientProxy) newHazelcastClient(asClientConfig(clientXml));
        }
    }

    public void destroy() {
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }
}

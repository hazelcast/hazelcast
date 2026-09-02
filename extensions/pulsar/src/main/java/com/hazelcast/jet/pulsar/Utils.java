/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.pulsar;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import org.apache.pulsar.client.api.PulsarClient;

final class Utils {

    private Utils() {
    }

    static PulsarClient getClient(Processor.Context ctx,
                                  SupplierEx<PulsarClient> clientSupplier,
                                  DataConnectionRef dataConnectionRef) {
        if (clientSupplier != null) {
            return clientSupplier.get();
        } else if (dataConnectionRef != null) {
            var dataConnection = ctx.dataConnectionService()
                                    .getAndRetainDataConnection(dataConnectionRef.getName(),
                                                                PulsarDataConnection.class);
            try {
                return dataConnection.getClient();
            } finally {
                dataConnection.release();
            }
        } else {
            throw new IllegalArgumentException("Either clientSupplier or DataConnectionRef must be set");
        }
    }

    static boolean exactlyOnlyOneIsNotNull(Object first, Object second) {
        return (first == null) != (second == null);
    }
}

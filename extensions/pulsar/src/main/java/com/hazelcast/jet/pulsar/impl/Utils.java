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
package com.hazelcast.jet.pulsar.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.spi.annotation.PrivateApi;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import static com.hazelcast.jet.impl.util.JetExceptionUtil.peel;

/**
 * Utility methods to work with Pulsar
 */
@PrivateApi
public final class Utils {
    public static final int MAX_TOPICS_LENGTH = 200;

    private Utils() {
    }

    public static PulsarClient getClient(ProcessorSupplier.Context ctx,
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

    public static boolean exactlyOnlyOneIsNotNull(Object first, Object second) {
        return (first == null) != (second == null);
    }

    /**
     * Wraps failure in either {@link JetException} or {@link RestartableException}.
     */
    public static JetException wrapFailure(String message, Throwable cause0) {
        var cause = peel(cause0);
        // A previous execution may still be closing its exclusive consumer after member loss.
        if (cause instanceof PulsarClientException.ConsumerBusyException || PulsarClientException.isRetriableError(cause)) {
            return new RestartableException(message, cause);
        }
        return new JetException(message, cause);
    }
}

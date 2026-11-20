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

import com.hazelcast.spi.annotation.PrivateApi;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Map;

import static com.hazelcast.jet.pulsar.PulsarProperties.filterOptions;

@SuppressWarnings({"rawtypes"})
@PrivateApi
public final class ConnectionSuppliers {

    private ConnectionSuppliers() {
    }
    public static PulsarClient createClient(String brokerUrl,
                                            boolean enableTransactions,
                                            Map options) throws PulsarClientException {
        return PulsarClient.builder()
                           .serviceUrl(brokerUrl)
                           .enableTransaction(enableTransactions)
                           .loadConf(filterOptions(options,
                                                   PulsarProperties.Prefixes.CLIENT))
                           .build();
    }

    public static PulsarAdmin createAdmin(String httpServiceUrl,
                                          Map options) throws PulsarClientException {
        return PulsarAdmin.builder()
                          .serviceHttpUrl(httpServiceUrl)
                          .loadConf(filterOptions(options, PulsarProperties.Prefixes.ADMIN))
                          .build();
    }

}

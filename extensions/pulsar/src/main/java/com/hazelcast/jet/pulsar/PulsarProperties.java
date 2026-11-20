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

import org.apache.pulsar.client.api.PulsarClient;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.hazelcast.jet.pulsar.ConnectionSuppliers.ADMIN_PREFIX;
import static com.hazelcast.jet.pulsar.ConnectionSuppliers.CLIENT_PREFIX;

/**
 * Properties that are possible to use with {@link PulsarDataConnection}.
 */
@SuppressWarnings("checkstyle:LineLength")
public final class PulsarProperties {
    /**
     * Pulsar Broker URL. Always mandatory.
     */
    public static final String BROKER_URL = "brokerUrl";

    /**
     * Pulsar HTTP endpoint URL for administrative tasks, like listing resources or checking topic deduplication status.
     */
    public static final String HTTP_SERVICE_URL = "httpServiceUrl";

    /**
     * Either localName ("namespace") or full name ("tenant/namespace").
     * Optional property. If set, it will filter only resources assigned to this namespace.
     */
    public static final String NAMESPACE = "namespace";

    /**
     * Tenant name, optional. If set, it will filter only resources assigned to this tenant.
     */
    public static final String TENANT = "tenant";

    /**
     * If true, transactions will be activated on underlying PulsarClient.
     * Please note, that transactions must be also enabled on broker side.
     * <p>
     * This option is necessary if given data connection will be used in Jet job with Pulsar Sink and
     * {@link com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE} processing guarantee.
     */
    public static final String ENABLE_TRANSACTIONS = "enableTransactions";

    /**
     * How long {@link PulsarDataConnection#destroy()} waits for the underlying shared
     * {@link PulsarClient} to close gracefully before falling back to
     * {@link PulsarClient#shutdown()}. Optional, in seconds; defaults to 5 minutes.
     */
    public static final String CLOSE_TIMEOUT_SECONDS = "closeTimeoutSeconds";

    private PulsarProperties() {
    }

    public static Map<String, String> stripPrefix(Map<String, String> options, String additionalPrefix) {
        var map = new LinkedHashMap<String, String>(options.size());

        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(additionalPrefix) && !key.startsWith("pulsar.")) {
                map.put(key, entry.getValue());
            }
        }
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("pulsar.")  && !(key.startsWith(ADMIN_PREFIX) || key.startsWith(CLIENT_PREFIX))) {
                map.put(key.replaceFirst("pulsar.", ""), entry.getValue());
            }
        }
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(additionalPrefix)) {
                map.put(key.replaceFirst(additionalPrefix, ""), entry.getValue());
            }
        }
        return map;
    }

    public static String clientOption(String optionName, Map<String, String> options) {
        return getOption(optionName, options, CLIENT_PREFIX);
    }

    public static String getOption(String optionName, Map<String, String> options, String additionalPrefix) {
       if (options.containsKey(additionalPrefix + optionName)) {
           return options.get(additionalPrefix + optionName);
       } else if (options.containsKey("pulsar." + optionName)) {
           return options.get("pulsar." + optionName);
       } else if (options.containsKey(optionName)) {
           return options.get(optionName);
       }
       return null;
   }
}

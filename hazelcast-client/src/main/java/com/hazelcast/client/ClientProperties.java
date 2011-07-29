/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public final class ClientProperties {

    public static enum ClientPropertyName {
        GROUP_NAME("hazelcast.client.group.name", null),
        GROUP_PASSWORD("hazelcast.client.group.password", null),
        INIT_CONNECTION_ATTEMPTS_LIMIT("hazelcast.client.init.connection.attempts.limit", "5"),
        RECONNECTION_ATTEMPTS_LIMIT("hazelcast.client.reconnection.attempts.limit", "5"),
        CONNECTION_TIMEOUT("hazelcast.client.connection.timeout", "300000"),
        RECONNECTION_TIMEOUT("hazelcast.client.reconnection.timeout", "5000");

        private final String name;
        private final String defaultValue;

        private ClientPropertyName(final String name, final String defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        public String getName() {
            return name;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public static ClientPropertyName fromValue(final String name) {
            for (final ClientPropertyName clientPropertyName : ClientPropertyName.values()) {
                if (clientPropertyName.getName().equals(name)) {
                    return clientPropertyName;
                }
            }
            throw new IllegalArgumentException("There is no client property that has name '" + name + "'");
        }
    }

    private final Map<ClientPropertyName, String> properties;

    public ClientProperties() {
        this.properties = new HashMap<ClientProperties.ClientPropertyName, String>();
    }

    public Map<ClientPropertyName, String> getProperties() {
        return this.properties;
    }

    public ClientProperties setProperties(final Map<String, String> properties) {
        for (final Entry<String, String> entry : properties.entrySet()) {
            setPropertyValue(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public ClientProperties setPropertyValue(final String name, final String value) {
        return setPropertyValue(ClientPropertyName.fromValue(name), value);
    }

    public ClientProperties setPropertyValue(final ClientPropertyName name, final String value) {
        this.properties.put(name, value);
        return this;
    }

    public String getProperty(final String name) {
        return getProperty(ClientPropertyName.fromValue(name));
    }

    public String getProperty(final ClientPropertyName name) {
        String string = this.properties.get(name);
        if (string == null) {
            string = System.getProperty(name.getName());
        }
        if (string == null) {
            string = name.defaultValue;
        }
        if (string == null) {
            throw new IllegalStateException("property " + name.getName() + " is null");
        }
        return string;
    }

    public int getInteger(final ClientPropertyName name) {
        return Integer.parseInt(getProperty(name));
    }

    public long getLong(final ClientPropertyName name) {
        return Long.parseLong(getProperty(name));
    }

    public static ClientProperties crateBaseClientProperties(final String groupName, final String groupPassword) {
        ClientProperties clientProperties = new ClientProperties();
        clientProperties.setPropertyValue(ClientPropertyName.GROUP_NAME, groupName);
        clientProperties.setPropertyValue(ClientPropertyName.GROUP_PASSWORD, groupPassword);
        return clientProperties;
    }
}

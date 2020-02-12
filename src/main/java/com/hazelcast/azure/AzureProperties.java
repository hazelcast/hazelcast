/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.azure;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;

import static com.hazelcast.config.properties.PropertyTypeConverter.BOOLEAN;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 * Configuration properties for the Hazelcast Discovery Plugin for Azure
 */
enum AzureProperties {

    CLIENT("client", BOOLEAN, true, false),
    /**
     * The constant TENANT_ID.
     */
    TENANT_ID("tenant-id", STRING, true),

    /**
     * The constant SUBSCRIPTION_ID.
     */
    SUBSCRIPTION_ID("subscription-id", STRING, true),

    /**
     * The constant CLIENT_ID.
     */
    CLIENT_ID("client-id", STRING, true),

    /**
     * The constant CLIENT_SECRET.
     */
    CLIENT_SECRET("client-secret", STRING, true),

    /**
     * Name of the Azure resource group that VMs/SM Scale Set created.
     * If not specified, then the "resource-group" is than from the Azure VM instance.
     * Clients connecting outside of Azure or from a different resource group should define the correct
     * "resource-group"
     */
    RESOURCE_GROUP("resource-group", STRING, true),

    /**
     * The constant CLIENT_SECRET.
     */
    SCALE_SET("scale-set", STRING, true),

    /**
     * A tag to limit the instance discovery. Format: "key=value".
     * <p>
     * If not specified, then "tag" is not used to filter instances.
     */
    TAG("tag", STRING, true),

    /**
     * Port range where Hazelcast is expected to be running. Format: "5701" or "5701-5703".
     * <p>
     * The default value is "5701-5703".
     */
    PORT("hz-port", STRING, true, "5701-5703");

    private final PropertyDefinition propertyDefinition;
    private final Comparable defaultValue;

    AzureProperties(String key, PropertyTypeConverter typeConverter, boolean optional, Comparable defaultValue) {
        this.propertyDefinition = new SimplePropertyDefinition(key, optional, typeConverter);
        this.defaultValue = defaultValue;
    }

    AzureProperties(String key, PropertyTypeConverter typeConverter, boolean optional) {
        this(key, typeConverter, optional, null);
    }

    PropertyDefinition getDefinition() {
        return propertyDefinition;
    }

    Comparable getDefaultValue() {
        return defaultValue;
    }
}

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

import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 * Configuration properties for the Hazelcast Discovery Plugin for Azure
 */
enum AzureProperties {

    /**
     * The tenant-id of the Azure account. This property should be provided with <code>client-id<code/>
     * and <code>client-secret</code>. If none of them are provided, then the authentication is tried to be done using
     * Azure VM instance metadata service.
     */
    TENANT_ID("tenant-id", STRING, true),

    /**
     * The client-id of the Azure account. This property should be provided with <code>tenant-id<code/>
     * and <code>client-secret</code>. If none of them are provided, then the authentication is tried to be done using
     * Azure VM instance metadata service.
     */
    CLIENT_ID("client-id", STRING, true),

    /**
     * The client-secret of the Azure account. This property should be provided with <code>tenant-id<code/>
     * and <code>client-id</code>. If none of them are provided, then the authentication is tried to be done using
     * Azure VM instance metadata service.
     */
    CLIENT_SECRET("client-secret", STRING, true),

    /**
     * ID of the Azure subscription that VMs/SM Scale Set created.
     * If not specified, then the <code>subscription-id</code> is taken from the Azure VM instance metadata service.
     * Instances connecting outside of Azure or from a different resource group should define the correct
     * <code>subscription-id</code>
     */
    SUBSCRIPTION_ID("subscription-id", STRING, true),

    /**
     * Name of the Azure resource group that VMs/SM Scale Set created.
     * If not specified, then the <code>resource-group</code> is taken from the Azure VM instance metadata service.
     * Instances connecting outside of Azure or from a different resource group should define the correct
     * <code>resource-group</code>
     */
    RESOURCE_GROUP("resource-group", STRING, true),

    /**
     * Name of the Azure VM scale set that VMs are created.
     * If not specified, then the <code>scale-set</code> is taken from the Azure VM instance metadata service.
     * Instances connecting outside of Azure or from a different resource group should define the correct
     * <code>scale-set</code>.
     * <p>
     * Please note that the discovery will be performed only in this scale-set when this property
     * has a value.
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

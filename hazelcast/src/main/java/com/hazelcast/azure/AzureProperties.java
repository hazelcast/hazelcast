/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

    /**
     * The tenant-id of the Azure account. This property can be used only if <code>instance-metadata-available</code> property
     * is set to <code>false</code>. Otherwise, the authentication token will be retrieved the Azure VM instance metadata service.
     */
    TENANT_ID("tenant-id", STRING, true),

    /**
     * The client-id of the Azure account. This property can be used only if <code>instance-metadata-available</code> property
     * is set to <code>false</code>. Otherwise, the authentication token will be retrieved the Azure VM instance metadata service.
     */
    CLIENT_ID("client-id", STRING, true),

    /**
     * The client-secret of the Azure account. This property can be used only if <code>instance-metadata-available</code> property
     * is set to <code>false</code>. Otherwise, the authentication token will be retrieved the Azure VM instance metadata service.
     */
    CLIENT_SECRET("client-secret", STRING, true),

    /**
     * ID of the Azure subscription that VMs/SM Scale Set created.
     * This property can be used only if <code>instance-metadata-available</code> property is set to <code>false</code>.
     * Otherwise, the <code>subscription-id</code> will be retrieved the Azure VM instance metadata service.
     */
    SUBSCRIPTION_ID("subscription-id", STRING, true),

    /**
     * Name of the Azure resource group that VMs/SM Scale Set created.
     * This property can be used only if <code>instance-metadata-available</code> property is set to <code>false</code>.
     * Otherwise, the <code>resource-group</code> will be retrieved the Azure VM instance metadata service.
     */
    RESOURCE_GROUP("resource-group", STRING, true),

    /**
     * Name of the Azure VM scale set that VMs are created.
     * This property can be used only if <code>instance-metadata-available</code> property is set to <code>false</code>.
     * Otherwise, the <code>scale-set</code> will be retrieved the Azure VM instance metadata service.
     * <p>
     * Please note that the discovery will be performed only in this scale-set when this property has a value.
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
    PORT("hz-port", STRING, true, "5701-5703"),

    /**
     * Property to enable/disable usage of Azure Instance Metadata service when retrieving current configuration parameters.
     * Should be set to false when using the plugin outside of Azure Environment or Azure Instance Metadata service is not
     * available.
     * <p/>
     * When set to <code>false<code/>, ALL of <code>tenantId</code>, <code>clientId</code>, <code>clientSecret</code>,
     * <code>subscriptionId</code>, and <code>resourceGroup</code> properties should be configured. When set to <code>true<code>,
     * NONE of <code>tenantId</code>, <code>clientId</code>, <code>clientSecret</code>, <code>subscriptionId</code>,
     * <code>resourceGroup</code>, and <code>scaleSet</code> properties should be configured.
     * <p/>
     * The default value is <code>true</code>.
     */
    INSTANCE_METADATA_AVAILABLE("instance-metadata-available", BOOLEAN, true, Boolean.TRUE);

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

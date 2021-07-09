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

package com.hazelcast.gcp;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;

import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 * Configuration properties for the Hazelcast Discovery Plugin for GCP.
 */
enum GcpProperties {

    /**
     * Filesystem path to the JSON file with the Service Account private key.
     * <p>
     * If not specified, then the access token is taken from the GCP VM Instance.
     */
    PRIVATE_KEY_PATH("private-key-path", STRING, true),

    /**
     * A list of GCP projects in the form of "project1,project2,project3".
     * <p>
     * If not specified, then the discovery takes place only in the current project.
     */
    PROJECTS("projects", STRING, true),

    /**
     * A list of GCP zones in the form of "zone1,zone2,zone3".
     * <p>
     * If not specified, then the discovery takes place in all zones of the current region.
     */
    ZONES("zones", STRING, true),

    /**
     * A label to limit the instance discovery. Format: "key=value".
     * <p>
     * If not specified, then "label" is not used to filter instances.
     */
    LABEL("label", STRING, true),

    /**
     * Port range where Hazelcast is expected to be running. Format: "5701" or "5701-5710".
     * <p>
     * The default value is "5701-5708".
     */
    PORT("hz-port", STRING, true, "5701-5708"),

    /**
     * A GCP region.
     * <p>
     * If not specified, then the discovery uses "zones" property.
     */
    REGION("region", STRING, true);

    private final PropertyDefinition propertyDefinition;
    private final Comparable defaultValue;

    GcpProperties(String key, PropertyTypeConverter typeConverter, boolean optional, Comparable defaultValue) {
        this.propertyDefinition = new SimplePropertyDefinition(key, optional, typeConverter);
        this.defaultValue = defaultValue;
    }

    GcpProperties(String key, PropertyTypeConverter typeConverter, boolean optional) {
        this(key, typeConverter, optional, null);
    }

    PropertyDefinition getDefinition() {
        return propertyDefinition;
    }

    Comparable getDefaultValue() {
        return defaultValue;
    }
}

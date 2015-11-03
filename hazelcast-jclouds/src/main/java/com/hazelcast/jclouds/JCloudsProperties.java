/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jclouds;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;

import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 *  Defines the name and default value for JCloud properties
 */
public final class JCloudsProperties {

    /**
     * Unique identifier for ComputeService Provider
     * see the full list of ids : https://jclouds.apache.org/reference/providers/
     */
    public static final PropertyDefinition PROVIDER = property("provider", STRING);

    /**
     * Unique credential identity specific to users cloud account
     */
    public static final PropertyDefinition IDENTITY = property("identity", STRING);
    /**
     * Unique credential specific to users cloud accounts identity
     */
    public static final PropertyDefinition CREDENTIAL = property("credential", STRING);
    /**
     * Property used to define zones for node filtering
     */
    public static final PropertyDefinition ZONES = property("zones", STRING);
    /**
     * Property used to define regions for node filtering
     */
    public static final PropertyDefinition REGIONS = property("regions", STRING);
    /**
     * Property used to define tag keys for node filtering, supports comma separated values
     */
    public static final PropertyDefinition TAG_KEYS = property("tag-keys", STRING);
    /**
     * Property used to define tag values for node filtering, supports comma separated values
     */
    public static final PropertyDefinition TAG_VALUES = property("tag-values", STRING);
    /**
     * Property used to define group keys for node filtering
     */
    public static final PropertyDefinition GROUP = property("group", STRING);
    /**
     * Property used to define which port hazelcast instance should be discovered in cloud
     * Default value is 5701,defined DEFAULT_PORT in {@link NetworkConfig}
     */
    public static final PropertyDefinition HZ_PORT = property("hz-port", INTEGER, new PortValueValidator());
    /**
     * Property used to define JSON credentials file specific to Google Compute Engine
     */
    public static final PropertyDefinition CREDENTIAL_PATH = property("credentialPath", STRING);
    /**
     * Property used to define IAM roles specific to AWS-EC2
     */
    public static final PropertyDefinition ROLE_NAME = property("role-name", STRING);

    private static final int MIN_PORT = 0;
    private static final int MAX_PORT = 65535;

    private JCloudsProperties() {
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return property(key, typeConverter, null);
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter,
                                               ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, true, typeConverter, valueValidator);
    }

    /**
     * Validator for valid network ports
     */
    protected static class PortValueValidator implements ValueValidator<Integer> {

        public void validate(Integer value) throws ValidationException {
            if (value < MIN_PORT) {
                throw new ValidationException("hz-port number must be greater 0");
            }
            if (value > MAX_PORT) {
                throw new ValidationException("hz-port number must be less or equal to 65535");
            }
        }
    }

}

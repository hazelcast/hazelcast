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

package com.hazelcast.spi.discovery.multicast;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValueValidator;

import static com.hazelcast.config.properties.PropertyTypeConverter.BOOLEAN;
import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 * Defines the name and default value for the Multicast Discovery Strategy.
 */
public final class MulticastProperties {

    /**
     * Property used to define multicast port.
     */
    public static final PropertyDefinition PORT = property("port", INTEGER);

    /**
     * Property used to define zones for node filtering.
     */
    public static final PropertyDefinition GROUP = property("group", STRING);

    /**
     * Property which determines if Java Serialization is used ({@code false}) or rather the safer and portable one
     * ({@code true}). The Java native serialization format which is used by default is sensitive to
     * <a href="https://owasp.org/www-project-top-ten/2017/A8_2017-Insecure_Deserialization">insecure deserialization
     * attacks</a>.
     * <p/>
     * Safe serialization format of the MulticastMemberInfo:
     *
     * <pre>
     * boolean (1b):              flag if memberInfo provided (false mean memberInfo is null)
     * UTF-8   (variable length): host
     * int:    (4b):              port
     * </pre>
     *
     * <b>UTF-8 format notes (see {@link java.io.DataOutputStream#writeUTF(String)}):<b>
     * <p/>
     * Two bytes (short) provides subsequent length to read. This value is the number of bytes actually, not the length of the
     * string. Modified UTF-8 is used for decoding.
     *
     * @since 5.1
     */
    public static final PropertyDefinition SAFE_SERIALIZATION = property("safe-serialization", BOOLEAN);

    private MulticastProperties() {
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return property(key, typeConverter, null);
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter, ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, true, typeConverter, valueValidator);
    }
}

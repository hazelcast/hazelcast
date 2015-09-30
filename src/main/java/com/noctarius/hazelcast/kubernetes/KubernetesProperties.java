/*
 * Copyright (c) 2015, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
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
package com.noctarius.hazelcast.kubernetes;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.TypeConverter;

import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

public final class KubernetesProperties {

    public static final PropertyDefinition SERVICE_DNS = property("service-dns", STRING);
    public static final PropertyDefinition SERVICE_DNS_IP_TYPE = property("service-dns-ip-type", new IpTypeConverter());
    public static final PropertyDefinition SERVICE_NAME = property("service-name", STRING);
    public static final PropertyDefinition NAMESPACE = property("namespace", STRING);

    // Prevent instantiation
    private KubernetesProperties() {
    }

    private static PropertyDefinition property(String key, TypeConverter typeConverter) {
        return new SimplePropertyDefinition(key, true, typeConverter);
    }

    public enum IpType {
        IPV4,
        IPV6
    }

    private static class IpTypeConverter implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (!(value instanceof String)) {
                throw new RuntimeException("Cannot convert from type '" + value.getClass() + "'");
            }

            String v = (String) value;
            if (v == null || v.length() == 0) {
                return IpType.IPV4;
            }

            IpType ipType = IpType.valueOf(v);
            if (ipType == null) {
                throw new RuntimeException("service-name-ip-type must either be IPV4, IPV6 or empty");
            }

            return ipType;
        }
    }
}

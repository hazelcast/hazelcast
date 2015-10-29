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

/**
 * <p>Configuration class of the Hazelcast Discovery Plugin for <a href="http://kubernetes.io">Kubernetes</a>.</p>
 * <p>For possible configuration properties please refer to the public constants of this class.</p>
 */
public final class KubernetesProperties {

    /**
     * <p>Configuration key: <tt>service-dns</tt></p>
     * Defines the DNS service lookup. This is defined as something similar to <tt>my-svc.my-namespace.svc.cluster.local</tt>.
     * For more information please refer to the official documentation of the Kubernetes DNS addon,
     * <a href="https://github.com/kubernetes/kubernetes/tree/v1.0.6/cluster/addons/dns">here</a>.
     */
    public static final PropertyDefinition SERVICE_DNS = property("service-dns", STRING);

    /**
     * <p>Configuration key: <tt>service-dns-ip-type</tt></p>
     * Defines the type of the IP address DNS entry (A for IPv4 or or AAAA for IPv6).<br/>
     * Possible values are:
     * <ul>
     * <li>IPV4</li>
     * <li>IPV6</li>
     * <li>empty or not available</li>
     * </ul>
     * <b>Defaults to IPV4</b>
     */
    public static final PropertyDefinition SERVICE_DNS_IP_TYPE = property("service-dns-ip-type", new IpTypeConverter());

    /**
     * <p>Configuration key: <tt>service-name</tt></p>
     * Defines the service name of the POD to lookup through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition SERVICE_NAME = property("service-name", STRING);

    /**
     * <p>Configuration key: <tt>namespace</tt></p>
     * Defines the namespace of the application POD through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition NAMESPACE = property("namespace", STRING);

    // Prevent instantiation
    private KubernetesProperties() {
    }

    private static PropertyDefinition property(String key, TypeConverter typeConverter) {
        return new SimplePropertyDefinition(key, true, typeConverter);
    }

    /**
     * Possible values for {@link #SERVICE_DNS_IP_TYPE}.
     */
    public enum IpType {
        /**
         * Activates IPV4 DNS record lookup
         */
        IPV4,

        /**
         * Activates IPV6 DNS record lookup
         */
        IPV6
    }

    private static class IpTypeConverter
            implements TypeConverter {

        public Comparable convert(Comparable value) {
            if (!(value instanceof String)) {
                throw new RuntimeException("Cannot convert from type '" + value.getClass() + "'");
            }

            String v = (String) value;
            if (v == null || v.length() == 0) {
                return IpType.IPV4;
            }

            IpType ipType = IpType.valueOf(v.toUpperCase());
            if (ipType == null) {
                throw new RuntimeException("service-name-ip-type must either be IPV4, IPV6 or empty");
            }

            return ipType;
        }
    }
}

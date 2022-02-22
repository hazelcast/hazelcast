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

package com.hazelcast.kubernetes;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.TypeConverter;

import static com.hazelcast.config.properties.PropertyTypeConverter.BOOLEAN;
import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 * <p>Configuration class of the Hazelcast Discovery Plugin for <a href="http://kubernetes.io">Kubernetes</a>.</p>
 * <p>For possible configuration properties please refer to the public constants of this class.</p>
 */
public final class KubernetesProperties {

    /**
     * <p>Configuration System Environment Prefix: <code>hazelcast.kubernetes.</code></p>
     * Defines the prefix for system environment variables and JVM command line parameters.<br>
     * Defining or overriding properties as JVM parameters or using the system environment, those
     * properties need to be prefixed to prevent collision on property names.<br>
     * Example: {@link #SERVICE_DNS} will be:
     * <pre>
     *     -Dhazelcast.kubernetes.service-dns=value
     * </pre>
     * For kubernetes and openshift there is a special rule where the environment variables are
     * provided in C-identifier style, therefore the prefix is converted to uppercase and dots
     * and dashed will be replaced with underscores:
     * <pre>
     *     HAZELCAST_KUBERNETES_SERVICE_DNS=value
     * </pre>
     */
    public static final String KUBERNETES_SYSTEM_PREFIX = "hazelcast.kubernetes.";

    /**
     * <p>Configuration key: <code>service-dns</code></p>
     * Defines the DNS service lookup domain. This is defined as something similar
     * to <code>my-svc.my-namespace.svc.cluster.local</code>.<br>
     * For more information please refer to the official documentation of the Kubernetes DNS addon,
     * <a href="https://github.com/kubernetes/kubernetes/tree/v1.0.6/cluster/addons/dns">here</a>.
     */
    public static final PropertyDefinition SERVICE_DNS = property("service-dns", STRING);

    /**
     * <p>Configuration key: <code>service-dns-timeout</code></p>
     * Defines the DNS service lookup timeout in seconds. Defaults to: 5 secs.
     */
    public static final PropertyDefinition SERVICE_DNS_TIMEOUT = property("service-dns-timeout", INTEGER);

    /**
     * <p>Configuration key: <code>service-name</code></p>
     * Defines the service name of the POD to lookup through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition SERVICE_NAME = property("service-name", STRING);
    /**
     * <p>Configuration key: <code>service-label-name</code></p>
     * Defines the service label to lookup through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition SERVICE_LABEL_NAME = property("service-label-name", STRING);
    /**
     * <p>Configuration key: <code>service-label-value</code></p>
     * Defines the service label value to lookup through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition SERVICE_LABEL_VALUE = property("service-label-value", STRING);

    /**
     * <p>Configuration key: <code>namespace</code></p>
     * Defines the namespace of the application POD through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition NAMESPACE = property("namespace", STRING);

    /**
     * <p>Configuration key: <code>pod-label-name</code></p>
     * Defines the pod label to lookup through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition POD_LABEL_NAME = property("pod-label-name", STRING);
    /**
     * <p>Configuration key: <code>pod-label-value</code></p>
     * Defines the pod label value to lookup through the Service Discovery REST API of Kubernetes.
     */
    public static final PropertyDefinition POD_LABEL_VALUE = property("pod-label-value", STRING);

    /**
     * <p>Configuration key: <code>expose-externally</code></p>
     * Specifies if Hazelcast should try to find its public addresses exposed with a single service.
     * If not set, Hazelcast tries to automatically find public addresses, but fails silently.
     * If <code>true</code>false, Hazelcast crashes if it can't find its public address.
     * If <code>false</code>, Hazelcast does not even try to find its public address.
     */
    public static final PropertyDefinition EXPOSE_EXTERNALLY = property("expose-externally", BOOLEAN);

    /**
     * <p>Configuration key: <code>service-per-pod-label-name</code></p>
     * Defines the label name of the service used to expose one Hazelcast pod (for the external smart client use case).
     */
    public static final PropertyDefinition SERVICE_PER_POD_LABEL_NAME = property("service-per-pod-label-name", STRING);
    /**
     * <p>Configuration key: <code>service-per-pod-label-value</code></p>
     * Defines the label value of the service used to expose one Hazelcast pod (for the external smart client use case).
     */
    public static final PropertyDefinition SERVICE_PER_POD_LABEL_VALUE = property("service-per-pod-label-value", STRING);

    /**
     * <p>Configuration key: <code>resolve-not-ready-addresses</code></p>
     * Defines if not ready addresses should be evaluated to be discovered on startup.
     */
    public static final PropertyDefinition RESOLVE_NOT_READY_ADDRESSES = property("resolve-not-ready-addresses", BOOLEAN);

    /**
     * <p>Configuration key: <code>use-node-name-as-external-address</code></p>
     * Defines if the node name should be used as external address, instead of looking up the external IP using
     * the <code>/nodes</code> resource. Default is false.
     */
    public static final PropertyDefinition USE_NODE_NAME_AS_EXTERNAL_ADDRESS = property("use-node-name-as-external-address",
            BOOLEAN);

    /**
     * <p>Configuration key: <code>kubernetes-api-retries</code></p>
     * Defines the number of retries to Kubernetes API. Defaults to: 3.
     */
    public static final PropertyDefinition KUBERNETES_API_RETIRES = property("kubernetes-api-retries", INTEGER);

    /**
     * <p>Configuration key: <code>kubernetes-master</code></p>
     * Defines an alternative address for the kubernetes master. Defaults to: <code>https://kubernetes.default.svc</code>
     */
    public static final PropertyDefinition KUBERNETES_MASTER_URL = property("kubernetes-master", STRING);

    /**
     * <p>Configuration key: <code>api-token</code></p>
     * Defines an oauth token for the kubernetes client to access the kubernetes REST API. Defaults to reading the
     * token from the auto-injected file at: <code>/var/run/secrets/kubernetes.io/serviceaccount/token</code>
     */
    public static final PropertyDefinition KUBERNETES_API_TOKEN = property("api-token", STRING);

    /**
     * Configuration key: <code>ca-certificate</code>
     * CA Authority certificate from Kubernetes Master, defaults to reading the certificate from the auto-injected file at:
     * <code>/var/run/secrets/kubernetes.io/serviceaccount/ca.crt</code>
     */
    public static final PropertyDefinition KUBERNETES_CA_CERTIFICATE = property("ca-certificate", STRING);

    /**
     * <p>Configuration key: <code>service-port</code></p>
     * If specified with a value greater than 0, its value defines the endpoint port of the service (overriding the default).
     */
    public static final PropertyDefinition SERVICE_PORT = property("service-port", INTEGER);

    // Prevent instantiation
    private KubernetesProperties() {
    }

    private static PropertyDefinition property(String key, TypeConverter typeConverter) {
        return new SimplePropertyDefinition(key, true, typeConverter);
    }
}

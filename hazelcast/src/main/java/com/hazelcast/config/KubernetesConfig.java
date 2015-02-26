/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import static com.hazelcast.util.ValidationUtil.hasText;

/**
 * The KubernetesConfig contains the configuration for Kubernetes join mechanism.
 * <p/>
 * what happens behind the scenes is that data about the running Kubernetes pods are downloaded using the
 * as potential Hazelcast members.
 * <p/>
 * <h1>Filtering</h1>
 * Pods can be filtered by using the label query, such as <code>name=hazelcast</code>
 * <p/>
 * Once Hazelcast has figured out which containers are available, it will use the private ip addresses of these
 * instances to create a tcp/ip-cluster.
 */
public class KubernetesConfig {

    private static final int CONNECTION_TIMEOUT = 5;
    private static final String DEFAULT_API_VERSION = "v1beta1";

    private boolean enabled;
    private String host;
    private String port;
    private String version = DEFAULT_API_VERSION;
    private String labelQuery;
    private int connectionTimeoutSeconds = CONNECTION_TIMEOUT;

    /**
     * Returns the Kubernetes API server host.
     * If no initial values set, read the value from environmental variable <code>KUBERNETES_RO_SERVICE_HOST</code>
     *
     * @return Kubernetes API server host
     */
    public String getHost() {
        if (host == null) {
            return System.getenv("KUBERNETES_RO_SERVICE_HOST");
        } else {
            return host;
        }
    }

    /**
     * Sets the Kubernetes API server host.
     *
     * @param host Kubernetes API server host.
     * @return the updated KubernetesConfig
     * @throws IllegalArgumentException if host is null or empty.
     */
    public KubernetesConfig setHost(String host) {
        this.host = hasText(host, "host");
        return this;
    }

    /**
     * Returns the Kubernetes API server port.
     * If no initial values set, read the value from environmental variable <code>KUBERNETES_RO_SERVICE_PORT</code>
     *
     * @return Kubernetes API server port
     */
    public String getPort() {
        if (port == null) {
            return System.getenv("KUBERNETES_RO_SERVICE_PORT");
        } else {
            return port;
        }
    }

    /**
     * Sets the Kubernetes API server port.
     *
     * @param port Kubernetes API server port.
     * @return the updated KubernetesConfig
     * @throws IllegalArgumentException if port is null or empty.
     */
    public KubernetesConfig setPort(String port) {
        this.port = hasText(port, "port");
        return this;
    }

    /**
     * Returns the Kubernetes API version.
     *
     * @return Kubernetes API version
     */
    public String getVersion() {
       return version;
    }

    /**
     * Sets the Kubernetes API version.
     *
     * @param version Kubernetes API version.
     * @return the updated KubernetesConfig
     * @throws IllegalArgumentException if version is null or empty.
     */
    public KubernetesConfig setVersion(String version) {
        this.version = hasText(version, "version");
        return this;
    }

    /**
     * Returns the label query.
     *
     * @return label query
     */
    public String getLabelQuery() {
        return labelQuery;
    }

    /**
     * Sets the label query.  Such as <code>name=hazelcast</code>
     *
     * @param labelQuery label query.
     * @return the updated KubernetesConfig
     * @throws IllegalArgumentException if labelQuery is null or empty.
     */
    public KubernetesConfig setLabelQuery(String labelQuery) {
        this.labelQuery = hasText(labelQuery, "labelQuery");
        return this;
    }

    /**
     * Enables or disables the Kubernetes join mechanism.
     *
     * @param enabled true if enabled, false otherwise.
     * @return the updated KubernetesConfig.
     */
    public KubernetesConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Checks if the Kubernetes join mechanism is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Gets the connection timeout in seconds.
     *
     * @return the connectionTimeoutSeconds; connection timeout in seconds
     * @see #setConnectionTimeoutSeconds(int)
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * Sets the connect timeout in seconds. See {@link TcpIpConfig#setConnectionTimeoutSeconds(int)} for more information.
     *
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds (connection timeout in seconds) to set
     * @return the updated AwsConfig.
     * @see #getConnectionTimeoutSeconds()
     * @see TcpIpConfig#setConnectionTimeoutSeconds(int)
     */
    public KubernetesConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        if (connectionTimeoutSeconds < 0) {
            throw new IllegalArgumentException("connection timeout can't be smaller than 0");
        }
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KubernetesConfig{");
        sb.append("enabled=").append(enabled);
        sb.append(", host='").append(getHost()).append('\'');
        sb.append(", port='").append(getPort()).append('\'');
        sb.append(", labelQuery='").append(labelQuery).append('\'');
        sb.append(", connectionTimeoutSeconds=").append(connectionTimeoutSeconds);
        sb.append('}');
        return sb.toString();
    }
}

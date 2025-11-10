/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.rest.RestConfig;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.config.RestEndpointGroup.getAllEndpointGroups;

/**
 * Server socket endpoint config specialized for REST service
 * Allows configuring access to REST groups similar to {@link RestApiConfig}
 *
 * @since 3.12
 *
 * @deprecated use RestConfig instead. Will be removed at 6.0.
 * @see RestConfig
 */
@Deprecated(since = "5.5", forRemoval = true)
@SuppressWarnings("checkstyle:methodcount")
public class RestServerEndpointConfig
        extends ServerSocketEndpointConfig {

    private final Set<Integer> enabledGroupCodes = Collections.synchronizedSet(new HashSet<>());

    public RestServerEndpointConfig() {
        for (RestEndpointGroup eg : getAllEndpointGroups()) {
            if (eg.isEnabledByDefault()) {
                enabledGroupCodes.add(eg.getCode());
            }
        }
    }

    @Override
    public final ProtocolType getProtocolType() {
        return ProtocolType.REST;
    }

    @Override
    public EndpointQualifier getQualifier() {
        return EndpointQualifier.REST;
    }

    /**
     * Enables all REST endpoint groups.
     */
    public RestServerEndpointConfig enableAllGroups() {
        return enableGroups(RestEndpointGroup.values());
    }

    /**
     * Enables provided REST endpoint groups. It doesn't replace already enabled groups.
     */
    public RestServerEndpointConfig enableGroups(RestEndpointGroup... endpointGroups) {
        if (endpointGroups != null) {
            enabledGroupCodes.addAll(Arrays.stream(endpointGroups).map(RestEndpointGroup::getCode).collect(Collectors.toSet()));
        }
        return this;
    }

    /**
     * Disables all REST endpoint groups.
     */
    public RestServerEndpointConfig disableAllGroups() {
        enabledGroupCodes.clear();
        return this;
    }

    /**
     * Disables provided REST endpoint groups.
     */
    public RestServerEndpointConfig disableGroups(RestEndpointGroup... endpointGroups) {
        if (endpointGroups != null) {
            Arrays.stream(endpointGroups).map(RestEndpointGroup::getCode).forEach(enabledGroupCodes::remove);
        }
        return this;
    }

    /**
     * Return true if the REST API is enabled and at least one REST endpoint group is allowed.
     */
    public boolean isEnabledAndNotEmpty() {
        return !enabledGroupCodes.isEmpty();
    }

    /**
     * Returns a not-{@code null} set of enabled REST endpoint groups.
     */
    public Set<RestEndpointGroup> getEnabledGroups() {
        return enabledGroupCodes.stream().map(RestEndpointGroup::getRestEndpointGroup).collect(Collectors.toSet());
    }

    /**
     * Checks if given REST endpoint group is enabled.
     * It can return {@code true} even if the REST API itself is disabled.
     */
    public boolean isGroupEnabled(RestEndpointGroup group) {
        return enabledGroupCodes.contains(group.getCode());
    }

    public RestServerEndpointConfig setEnabledGroups(Collection<RestEndpointGroup> groups) {
        enabledGroupCodes.clear();
        if (groups != null) {
            enabledGroupCodes.addAll(groups.stream().map(RestEndpointGroup::getCode).collect(Collectors.toSet()));
        }
        return this;
    }

    @Override
    public RestServerEndpointConfig setPublicAddress(String publicAddress) {
        super.setPublicAddress(publicAddress);
        return this;
    }

    @Override
    public RestServerEndpointConfig setPort(int port) {
        super.setPort(port);
        return this;
    }

    @Override
    public RestServerEndpointConfig setPortAutoIncrement(boolean portAutoIncrement) {
        super.setPortAutoIncrement(portAutoIncrement);
        return this;
    }

    @Override
    public RestServerEndpointConfig setReuseAddress(boolean reuseAddress) {
        super.setReuseAddress(reuseAddress);
        return this;
    }

    @Override
    public RestServerEndpointConfig setName(String name) {
        super.setName(name);
        return this;
    }

    @Override
    public RestServerEndpointConfig setOutboundPortDefinitions(Collection<String> outboundPortDefs) {
        super.setOutboundPortDefinitions(outboundPortDefs);
        return this;
    }

    @Override
    public RestServerEndpointConfig setOutboundPorts(Collection<Integer> outboundPorts) {
        super.setOutboundPorts(outboundPorts);
        return this;
    }

    @Override
    public RestServerEndpointConfig setInterfaces(InterfacesConfig interfaces) {
        super.setInterfaces(interfaces);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketBufferDirect(boolean socketBufferDirect) {
        super.setSocketBufferDirect(socketBufferDirect);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketKeepAlive(boolean socketKeepAlive) {
        super.setSocketKeepAlive(socketKeepAlive);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketTcpNoDelay(boolean socketTcpNoDelay) {
        super.setSocketTcpNoDelay(socketTcpNoDelay);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketSendBufferSizeKb(int socketSendBufferSizeKb) {
        super.setSocketSendBufferSizeKb(socketSendBufferSizeKb);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketRcvBufferSizeKb(int socketRcvBufferSizeKb) {
        super.setSocketRcvBufferSizeKb(socketRcvBufferSizeKb);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketLingerSeconds(int socketLingerSeconds) {
        super.setSocketLingerSeconds(socketLingerSeconds);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketConnectTimeoutSeconds(int socketConnectTimeoutSeconds) {
        super.setSocketConnectTimeoutSeconds(socketConnectTimeoutSeconds);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        super.setSocketInterceptorConfig(socketInterceptorConfig);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSSLConfig(SSLConfig sslConfig) {
        super.setSSLConfig(sslConfig);
        return this;
    }

    @Beta
    @Nonnull
    @Override
    public RestServerEndpointConfig setTpcSocketConfig(@Nonnull TpcSocketConfig tpcSocketConfig) {
        super.setTpcSocketConfig(tpcSocketConfig);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSymmetricEncryptionConfig(SymmetricEncryptionConfig symmetricEncryptionConfig) {
        super.setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        return this;
    }

    @Override
    public RestServerEndpointConfig addOutboundPortDefinition(String portDef) {
        super.addOutboundPortDefinition(portDef);
        return this;
    }

    @Override
    public RestServerEndpointConfig addOutboundPort(int port) {
        super.addOutboundPort(port);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketKeepIdleSeconds(int socketKeepIdleSeconds) {
        super.setSocketKeepIdleSeconds(socketKeepIdleSeconds);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketKeepIntervalSeconds(int socketKeepIntervalSeconds) {
        super.setSocketKeepIntervalSeconds(socketKeepIntervalSeconds);
        return this;
    }

    @Override
    public RestServerEndpointConfig setSocketKeepCount(int socketKeepCount) {
        super.setSocketKeepCount(socketKeepCount);
        return this;
    }

    @Override
    public String toString() {
        return "RestServerEndpointConfig{enabledGroups=" + getEnabledGroups() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RestServerEndpointConfig that = (RestServerEndpointConfig) o;
        return Objects.equals(enabledGroupCodes, that.enabledGroupCodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), enabledGroupCodes);
    }
}

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

import java.util.Collection;
import java.util.HashSet;

public class NetworkConfig {

    public static final int DEFAULT_PORT = 5701;

    private int port = DEFAULT_PORT;

    private int portCount = 100;

    private boolean portAutoIncrement = true;

    private boolean reuseAddress = false;

    private String publicAddress = null;

    private Collection<String> outboundPortDefinitions;

    private Collection<Integer> outboundPorts;

    private InterfacesConfig interfaces = new InterfacesConfig();

    private JoinConfig join = new JoinConfig();

    private SymmetricEncryptionConfig symmetricEncryptionConfig = null;

    private SocketInterceptorConfig socketInterceptorConfig = null;

    private SSLConfig sslConfig = null;

    public NetworkConfig() {
        String os = System.getProperty("os.name").toLowerCase();
        reuseAddress = (!os.contains("win"));
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port the port to set
     */
    public NetworkConfig setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * @return the maximum number of ports allowed to use.
     */
    public int getPortCount() {
        return portCount;
    }


    /**
     * @param portCount the maximum number of ports allowed to use.
     */
    public void setPortCount(int portCount) {
        this.portCount = portCount;
    }

    /**
     * @return the portAutoIncrement
     */
    public boolean isPortAutoIncrement() {
        return portAutoIncrement;
    }

    /**
     * @param portAutoIncrement the portAutoIncrement to set
     */
    public NetworkConfig setPortAutoIncrement(boolean portAutoIncrement) {
        this.portAutoIncrement = portAutoIncrement;
        return this;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public NetworkConfig setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    public Collection<String> getOutboundPortDefinitions() {
        return outboundPortDefinitions;
    }

    public NetworkConfig setOutboundPortDefinitions(final Collection<String> outboundPortDefs) {
        this.outboundPortDefinitions = outboundPortDefs;
        return this;
    }

    public NetworkConfig addOutboundPortDefinition(String portDef) {
        if (outboundPortDefinitions == null) {
            outboundPortDefinitions = new HashSet<String>();
        }
        outboundPortDefinitions.add(portDef);
        return this;
    }

    public Collection<Integer> getOutboundPorts() {
        return outboundPorts;
    }

    public NetworkConfig setOutboundPorts(final Collection<Integer> outboundPorts) {
        this.outboundPorts = outboundPorts;
        return this;
    }

    public NetworkConfig addOutboundPort(int port) {
        if (outboundPorts == null) {
            outboundPorts = new HashSet<Integer>();
        }
        outboundPorts.add(port);
        return this;
    }

    /**
     * @return the interfaces
     */
    public InterfacesConfig getInterfaces() {
        return interfaces;
    }

    /**
     * @param interfaces the interfaces to set
     */
    public NetworkConfig setInterfaces(final InterfacesConfig interfaces) {
        this.interfaces = interfaces;
        return this;
    }

    /**
     * @return the join
     */
    public JoinConfig getJoin() {
        return join;
    }

    /**
     * @param join the join to set
     */
    public NetworkConfig setJoin(final JoinConfig join) {
        this.join = join;
        return this;
    }
    
    public String getPublicAddress() {
		return publicAddress;
	}
    
    public NetworkConfig setPublicAddress(String publicAddress) {
		this.publicAddress = publicAddress;
        return this;
	}

    public NetworkConfig setSocketInterceptorConfig(SocketInterceptorConfig socketInterceptorConfig) {
        this.socketInterceptorConfig = socketInterceptorConfig;
        return this;
    }

    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return socketInterceptorConfig;
    }

    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return symmetricEncryptionConfig;
    }

    public NetworkConfig setSymmetricEncryptionConfig(final SymmetricEncryptionConfig symmetricEncryptionConfig) {
        this.symmetricEncryptionConfig = symmetricEncryptionConfig;
        return this;
    }

    public SSLConfig getSSLConfig() {
        return sslConfig;
    }

    public NetworkConfig setSSLConfig(SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NetworkConfig {");
        sb.append("publicAddress='").append(publicAddress).append('\'');
        sb.append(", port=").append(port);
        sb.append(", portCount=").append(portCount);
        sb.append(", portAutoIncrement=").append(portAutoIncrement);
        sb.append(", join=").append(join);
        sb.append(", interfaces=").append(interfaces);
        sb.append(", sslConfig=").append(sslConfig);
        sb.append(", socketInterceptorConfig=").append(socketInterceptorConfig);
        sb.append(", symmetricEncryptionConfig=").append(symmetricEncryptionConfig);
        sb.append('}');
        return sb.toString();
    }


}

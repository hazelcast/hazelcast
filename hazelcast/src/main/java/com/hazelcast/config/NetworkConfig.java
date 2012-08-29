/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.nio.DataSerializable;
import com.hazelcast.util.ByteUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NetworkConfig implements DataSerializable {

    public static final int DEFAULT_PORT = 5701;

    private int port = DEFAULT_PORT;

    private boolean portAutoIncrement = true;

    private boolean reuseAddress = false;

    private String publicAddress = null;

    private Interfaces interfaces = new Interfaces();

    private Join join = new Join();

    private SymmetricEncryptionConfig symmetricEncryptionConfig = null;

    private AsymmetricEncryptionConfig asymmetricEncryptionConfig = null;

    private SocketInterceptorConfig socketInterceptorConfig = null;

    private SSLConfig sslConfig = null;

    public NetworkConfig() {
        String os = System.getProperty("os.name").toLowerCase();
        reuseAddress = (os.indexOf("win") == -1);
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

    /**
     * @return the interfaces
     */
    public Interfaces getInterfaces() {
        return interfaces;
    }

    /**
     * @param interfaces the interfaces to set
     */
    public NetworkConfig setInterfaces(final Interfaces interfaces) {
        this.interfaces = interfaces;
        return this;
    }

    /**
     * @return the join
     */
    public Join getJoin() {
        return join;
    }

    /**
     * @param join the join to set
     */
    public NetworkConfig setJoin(final Join join) {
        this.join = join;
        return this;
    }
    
    public String getPublicAddress() {
		return publicAddress;
	}
    
    public void setPublicAddress(String publicAddress) {
		this.publicAddress = publicAddress;
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

    public AsymmetricEncryptionConfig getAsymmetricEncryptionConfig() {
        return asymmetricEncryptionConfig;
    }

    public NetworkConfig setAsymmetricEncryptionConfig(final AsymmetricEncryptionConfig asymmetricEncryptionConfig) {
        this.asymmetricEncryptionConfig = asymmetricEncryptionConfig;
        return this;
    }

    public SSLConfig getSSLConfig() {
        return sslConfig;
    }

    public NetworkConfig setSSLConfig(SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(port);
        interfaces.writeData(out);
        join.writeData(out);
        boolean hasSymmetricEncryptionConfig = symmetricEncryptionConfig != null;
        boolean hasAsymmetricEncryptionConfig = asymmetricEncryptionConfig != null;
        out.writeByte(ByteUtil.toByte(portAutoIncrement, reuseAddress,
                hasSymmetricEncryptionConfig, hasAsymmetricEncryptionConfig));
        if (hasSymmetricEncryptionConfig) {
            symmetricEncryptionConfig.writeData(out);
        }
        if (hasAsymmetricEncryptionConfig) {
            asymmetricEncryptionConfig.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        port = in.readInt();
        interfaces = new Interfaces();
        interfaces.readData(in);
        join = new Join();
        join.readData(in);
        boolean[] b = ByteUtil.fromByte(in.readByte());
        portAutoIncrement = b[0];
        reuseAddress = b[1];
        boolean hasSymmetricEncryptionConfig = b[2];
        boolean hasAsymmetricEncryptionConfig = b[3];
        if (hasSymmetricEncryptionConfig) {
            symmetricEncryptionConfig = new SymmetricEncryptionConfig();
            symmetricEncryptionConfig.readData(in);
        }
        if (hasAsymmetricEncryptionConfig) {
            asymmetricEncryptionConfig = new AsymmetricEncryptionConfig();
            asymmetricEncryptionConfig.readData(in);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NetworkConfig {");
        sb.append("publicAddress='").append(publicAddress).append('\'');
        sb.append(", port=").append(port);
        sb.append(", portAutoIncrement=").append(portAutoIncrement);
        sb.append(", join=").append(join);
        sb.append(", interfaces=").append(interfaces);
        sb.append(", sslConfig=").append(sslConfig);
        sb.append(", socketInterceptorConfig=").append(socketInterceptorConfig);
        sb.append(", symmetricEncryptionConfig=").append(symmetricEncryptionConfig);
        sb.append(", asymmetricEncryptionConfig=").append(asymmetricEncryptionConfig);
        sb.append('}');
        return sb.toString();
    }


}

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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.impl.NodeType;
import com.hazelcast.impl.spi.NoReply;
import com.hazelcast.impl.spi.NonMemberOperation;
import com.hazelcast.impl.spi.Operation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.security.Credentials;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinRequest extends Operation implements NonMemberOperation, NoReply {

    protected NodeType nodeType = NodeType.MEMBER;
    public Address address;
    public Address to;
    public byte packetVersion;
    public int buildNumber;
    public Config config;
    public String uuid;
    private Credentials credentials;

    public JoinRequest() {
        super();
    }

    public JoinRequest(Address address, Config config, NodeType type, byte packetVersion, int buildNumber, String nodeUuid) {
        this(null, address, config, type, packetVersion, buildNumber, nodeUuid);
    }

    public JoinRequest(Address to, Address address, Config config, NodeType type, byte packetVersion, int buildNumber, String nodeUuid) {
        super();
        this.to = to;
        this.address = address;
        this.config = config;
        this.nodeType = type;
        this.packetVersion = packetVersion;
        this.buildNumber = buildNumber;
        this.uuid = nodeUuid;
    }

    public void readInternal(DataInput in) throws IOException {
        packetVersion = in.readByte();
        buildNumber = in.readInt();
        boolean hasTo = in.readBoolean();
        if (hasTo) {
            to = new Address();
            to.readData(in);
        }
        address = new Address();
        address.readData(in);
        nodeType = NodeType.create(in.readInt());
        config = new Config();
        config.readData(in);
        uuid = in.readUTF();
        credentials = (Credentials) IOUtil.readObject(in);
        if (credentials != null) {
            credentials.setEndpoint(address.getHost());
        }
    }

    public void writeInternal(DataOutput out) throws IOException {
        out.writeByte(packetVersion);
        out.writeInt(buildNumber);
        boolean hasTo = (to != null);
        out.writeBoolean(hasTo);
        if (hasTo) {
            to.writeData(out);
        }
        address.writeData(out);
        out.writeInt(nodeType.getValue());
        config.writeData(out);
        out.writeUTF(uuid);
        IOUtil.writeObject(out, credentials);
    }

    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return "JoinRequest{"
                + "nodeType=" + nodeType
                + ", address=" + address
                + ", buildNumber='" + buildNumber + '\''
                + ", packetVersion='" + packetVersion + '\''
                + ", config='" + config + "'}";
    }
//    public void process() {
//        getNode().clusterManager.handleJoinRequest(this);
//    }

    public void run() {
        ClusterImpl cm = (ClusterImpl) getService();
        cm.handleJoinRequest(this);
    }
}

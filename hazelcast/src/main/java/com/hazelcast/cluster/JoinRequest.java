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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.Credentials;

import java.io.IOException;

public class JoinRequest extends JoinMessage implements DataSerializable {

    private Credentials credentials;
    private int tryCount = 0;

    public JoinRequest() {
        super();
    }

    public JoinRequest(byte packetVersion, int buildNumber, Address address, String uuid, Config config,
                       Credentials credentials, int memberCount, int tryCount) {
        super(packetVersion, buildNumber, address, uuid, config, memberCount);
        this.credentials = credentials;
        this.tryCount = tryCount;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        credentials = in.readObject();
        if (credentials != null) {
            credentials.setEndpoint(getAddress().getHost());
        }
        tryCount = in.readInt();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(credentials);
        out.writeInt(tryCount);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("JoinRequest");
        sb.append("{packetVersion=").append(packetVersion);
        sb.append(", buildNumber=").append(buildNumber);
        sb.append(", address=").append(address);
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", credentials=").append(credentials);
        sb.append(", memberCount=").append(memberCount);
        sb.append(", tryCount=").append(tryCount);
        sb.append('}');
        return sb.toString();
    }
}

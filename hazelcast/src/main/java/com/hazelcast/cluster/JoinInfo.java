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
import com.hazelcast.impl.NodeType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

public class JoinInfo extends JoinRequest implements DataSerializable {

    private boolean request = true;
    private int memberCount = 0;
    private int tryCount = 0;
    final ILogger logger;

    public JoinInfo() {
        this.logger = Logger.getLogger(JoinInfo.class.getName());
    }

    public JoinInfo(ILogger logger, boolean request, Address address, Config config,
                    NodeType type, byte packetVersion, int buildNumber, int memberCount, int tryCount, String nodeUuid) {
        super(address, config, type, packetVersion, buildNumber, nodeUuid);
        this.request = request;
        this.memberCount = memberCount;
        this.tryCount = tryCount;
        this.logger = logger;
    }

    public JoinInfo copy(boolean newRequest, Address newAddress, int memberCount) {
        return new JoinInfo(logger, newRequest, newAddress, config,
                nodeType, packetVersion, buildNumber, memberCount, tryCount, uuid);
    }

    @Override
    public void readData(DataInput dis) throws IOException {
        super.readData(dis);
        this.request = dis.readBoolean();
        memberCount = dis.readInt();
        tryCount = dis.readInt();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        try {
            super.writeData(out);
            out.writeBoolean(isRequest());
            out.writeInt(memberCount);
            out.writeInt(tryCount);
        } catch (IOException e) {
            logger.log(Level.FINEST, e.getMessage(), e);
        }
    }

    /**
     * @param request the request to set
     */
    public void setRequest(boolean request) {
        this.request = request;
    }

    /**
     * @return the request
     */
    public boolean isRequest() {
        return request;
    }

    public int getMemberCount() {
        return memberCount;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    @Override
    public String toString() {
        return "JoinInfo{" +
                "request=" + isRequest() +
                ", memberCount=" + memberCount +
                "  " + super.toString() +
                '}';
    }
}

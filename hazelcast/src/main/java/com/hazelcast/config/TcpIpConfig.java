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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.ByteUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class TcpIpConfig implements DataSerializable {

    private int connectionTimeoutSeconds = 5;

    private boolean enabled = false;

    private List<String> members = new ArrayList<String>();

    private String requiredMember = null;

    public TcpIpConfig addMember(final String member) {
        this.members.add(member);
        return this;
    }

    public TcpIpConfig clear() {
        members.clear();
        return this;
    }

    /**
     * @return the connectionTimeoutSeconds
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds to set
     */
    public TcpIpConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public TcpIpConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * @return the lsMembers
     */
    public List<String> getMembers() {
        if (members == null) {
            members = new ArrayList<String>();
        }
        return members;
    }

    /**
     * @param members the members to set
     */
    public TcpIpConfig setMembers(final List<String> members) {
        this.members.clear();
        for (String member : members) {
            StringTokenizer tokenizer = new StringTokenizer(member, ",");
            while (tokenizer.hasMoreTokens()) {
                String s = tokenizer.nextToken();
                this.members.add(s.trim());
            }
        }
        return this;
    }

    /**
     * @return the requiredMember
     */
    public String getRequiredMember() {
        return requiredMember;
    }

    /**
     * @param requiredMember the requiredMember to set
     */
    public TcpIpConfig setRequiredMember(final String requiredMember) {
        this.requiredMember = requiredMember;
        return this;
    }

    @Override
    public String toString() {
        return "TcpIpConfig [enabled=" + enabled
                + ", connectionTimeoutSeconds=" + connectionTimeoutSeconds
                + ", members=" + members
                + ", requiredMember=" + requiredMember
                + "]";
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        boolean hasMembers = members != null && !members.isEmpty();
        boolean hasRequiredMember = requiredMember != null;
        out.writeByte(ByteUtil.toByte(enabled, hasRequiredMember, hasMembers));
        out.writeInt(connectionTimeoutSeconds);
        if (hasRequiredMember) {
            out.writeUTF(requiredMember);
        }
        if (hasMembers) {
            out.writeInt(members.size());
            for (final String member : members) {
                out.writeUTF(member);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        boolean[] b = ByteUtil.fromByte(in.readByte());
        enabled = b[0];
        boolean hasRequiredMember = b[1];
        boolean hasMembers = b[2];
        connectionTimeoutSeconds = in.readInt();
        if (hasRequiredMember) {
            requiredMember = in.readUTF();
        }
        if (hasMembers) {
            int size = in.readInt();
            members = new ArrayList<String>(size);
            for (int i = 0; i < size; i++) {
                members.add(in.readUTF());
            }
        }
    }


}

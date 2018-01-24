/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.raft;

/**
 * Represents a member of the Raft group.
 * Each member must have a unique address and id in the group.
 */
public class RaftMember {

    private String address;
    private String uid;

    public RaftMember() {
    }

    public RaftMember(RaftMember member) {
        this(member.address, member.uid);
    }

    public RaftMember(String address, String uid) {
        this.address = address;
        this.uid = uid;
    }

    public String getAddress() {
        return address;
    }

    public RaftMember setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getUid() {
        return uid;
    }

    public RaftMember setUid(String uid) {
        this.uid = uid;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftMember)) {
            return false;
        }

        RaftMember that = (RaftMember) o;
        return address.equals(that.address) && uid.equals(that.uid);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + uid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RaftMember{" + "address='" + address + '\'' + ", id='" + uid + '\'' + '}';
    }
}

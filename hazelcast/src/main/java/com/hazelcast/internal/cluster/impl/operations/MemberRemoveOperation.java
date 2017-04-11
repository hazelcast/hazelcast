/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembershipManagerCompat;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

@Deprecated
// not used on 3.9+
public class MemberRemoveOperation extends AbstractClusterOperation {

    private Address address;
    private String memberUuid;

    public MemberRemoveOperation() {
    }

    public MemberRemoveOperation(Address address) {
        this.address = address;
    }

    public MemberRemoveOperation(Address address, String uuid) {
        this.address = address;
        this.memberUuid = uuid;
    }

    @Override
    public void run() {
        ClusterServiceImpl clusterService = getService();
        Address caller = getCallerAddress();
        ILogger logger = getLogger();

        if (!isCallerValid(caller)) {
            return;
        }

        String msg = "Removing member " + address + (memberUuid != null ? ", uuid: " + memberUuid : "")
                + ", requested by: " + caller;
        if (logger.isFineEnabled()) {
            logger.fine(msg);
        }

        MembershipManagerCompat membershipManagerCompat = clusterService.getMembershipManagerCompat();
        membershipManagerCompat.removeMember(address, memberUuid, msg);
    }

    private boolean isCallerValid(Address caller) {
        ClusterServiceImpl clusterService = getService();
        ILogger logger = getLogger();

        if (caller == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring removal request of " + address + ", because sender is local or not known.");
            }
            return false;
        }

        if (!address.equals(caller) && !caller.equals(clusterService.getMasterAddress())) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring removal request of " + address + ", because sender is neither dead-member "
                        + "nor master: " + caller);
            }
            return false;
        }
        return true;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        memberUuid = in.readUTF();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        out.writeUTF(memberUuid);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBER_REMOVE;
    }

}

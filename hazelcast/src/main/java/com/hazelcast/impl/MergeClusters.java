/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

public class MergeClusters extends AbstractRemotelyProcessable {

    private Address newTargetAddress = null;

    public MergeClusters() {
    }

    public MergeClusters(Address newTargetAddress) {
        super();
        this.newTargetAddress = newTargetAddress;
    }

    public void process() {
        if (conn == null) {
            return;
        }

        final Address endpoint = conn.getEndPoint();
        final Address masterAddress = node.getMasterAddress();
        final ILogger logger = node.loggingService.getLogger(this.getClass().getName());
        if (endpoint == null || !endpoint.equals(masterAddress)) {
            logger.log(Level.WARNING, "Merge instruction sent from non-master endpoint: " + endpoint);
            return;
        }

        node.getExecutorManager().executeNow(new Runnable() {
            public void run() {
                logger.log(Level.WARNING, node.address + " is merging to " + newTargetAddress
                                          + ", because: instructed by master " + masterAddress);
                node.getJoiner().setTargetAddress(newTargetAddress);
                node.factory.restart();
            }
        });
    }

    public void readData(DataInput in) throws IOException {
        super.readData(in);
        newTargetAddress = new Address();
        newTargetAddress.readData(in);
    }

    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        newTargetAddress.writeData(out);
    }
}

/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.JoinInfo;
import com.hazelcast.nio.Address;

public class NodeMulticastListener implements MulticastListener {
    final Node node;

    public NodeMulticastListener(Node node) {
        this.node = node;
    }

    public void onMessage(Object msg) {
        if (msg != null && msg instanceof JoinInfo) {
            JoinInfo joinInfo = (JoinInfo) msg;
            if (node.address != null && !node.address.equals(joinInfo.address)) {
                boolean validJoinRequest;
                try {
                    validJoinRequest = node.validateJoinRequest(joinInfo);
                } catch (Exception e) {
                    validJoinRequest = false;
                }
                if (validJoinRequest) {
                    if (node.isMaster() && node.isActive() && node.joined()) {
                        if (joinInfo.isRequest()) {
                            node.multicastService.send(joinInfo.copy(false, node.address));
                        }
                    } else {
                        if (!node.joined() && !joinInfo.isRequest()) {
                            if (node.masterAddress == null) {
                                node.masterAddress = new Address(joinInfo.address);
                            }
                        }
                    }
                }
            }
        }
    }
}

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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import java.util.logging.Level;

/**
 * @author mdogan 9/28/12
 */
public class MasterConfirmationOperation extends AbstractClusterOperation {

    public void run() {
        final Address endpoint = getCallerAddress();
        if (endpoint == null) {
            return;
        }
        final ClusterServiceImpl clusterService = getService();
        final ILogger logger = getNodeEngine().getLogger(MasterConfirmationOperation.class.getName());
        final MemberImpl member = clusterService.getMember(endpoint);
        if (member != null) {
            if (clusterService.isMaster()) {
                clusterService.acceptMasterConfirmation(member);
            } else {
                logger.warning(endpoint + " has sent MasterConfirmation, but this node is not master!");
            }
        } else {
            logger.warning("MasterConfirmation has been received from " + endpoint
                                      + ", but it is not a member of this cluster!");
            getNodeEngine().getOperationService().send(new MemberRemoveOperation(clusterService.getThisAddress()), endpoint);
        }
    }

}

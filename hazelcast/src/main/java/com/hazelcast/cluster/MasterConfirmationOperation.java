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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.AbstractOperation;

import java.util.logging.Level;

/**
 * @mdogan 9/28/12
 */
public class MasterConfirmationOperation extends AbstractOperation {

    public void run() {
        Address endpoint = getCaller();
        if (endpoint == null) {
            return;
        }
        ClusterService clusterService = getService();
        if (!clusterService.isMaster()) {
            final ILogger logger = getNodeService().getLogger(MasterConfirmationOperation.class.getName());
            logger.log(Level.WARNING, endpoint + " has sent MasterConfirmation, but this node is not master!");
            return;
        }
        clusterService.acceptMasterConfirmation(endpoint);
    }

}

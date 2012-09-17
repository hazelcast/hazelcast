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

package com.hazelcast.impl.cluster;

import com.hazelcast.impl.Node;
import com.hazelcast.impl.spi.AbstractOperation;
import com.hazelcast.impl.spi.NodeServiceImpl;
import com.hazelcast.logging.ILogger;

import java.util.logging.Level;

/**
 * @mdogan 9/14/12
 */
public class AuthenticationFailureOperation extends AbstractOperation
        implements JoinOperation {

    public void run() {
        final NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        final Node node = nodeService.getNode();
        final ILogger logger = nodeService.getLogger("com.hazelcast.security");
        logger.log(Level.SEVERE, "Authentication failed on master node! Node is going to shutdown now!");
        node.shutdown(true, true);
    }
}
/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

/**
 * Exception indicating that the master node of the cluster has changed during the process.
 * <p>
 * This can occur when a request is sent to the master node, but the node restarts,
 * and a new node comes up at the same address — however, it is no longer the master.
 * <p>
 * This exception is used to trigger retries.
 */
public class MasterNodeChangedException extends ClusterTopologyChangedException {

    public MasterNodeChangedException() {
    }

    public MasterNodeChangedException(String message) {
        super(message);
    }

}

/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

/**
 * Marker interface for operations which need to control whether the check
 * for quorum will be performed. Internal operations (not triggered by the
 * user) may need to skip the quorum check to conserve consistency of data.
 *
 * @see com.hazelcast.config.QuorumConfig
 * @see QuorumCheckAwareOperation
 * @see com.hazelcast.spi.ReadonlyOperation
 */
public interface QuorumCheckAwareOperation {

    /**
     * Returns {@code true} if the quorum check should be performed. Operations
     * which require a quorum check may get rejected if there are not enough
     * members in the cluster.
     */
    boolean shouldCheckQuorum();
}

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

package com.hazelcast.spi;

import com.hazelcast.nio.serialization.DataSerializable;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface ReplicaFragmentAwareService {

    // TODO: remove registered namespaces those don't exist in this set
    Collection<ReplicaFragmentNamespace> getAllFragmentNamespaces(PartitionReplicationEvent event);

    Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ReplicaFragmentNamespace> namespaces);

    interface ReplicaFragmentNamespace extends DataSerializable {
        /**
         * Name of the service which fragments belongs to
         * @return name of the service
         */
        String getServiceName();
    }

    interface ReplicaFragmentAware {
        ReplicaFragmentNamespace getReplicaFragmentNamespace();
    }
}

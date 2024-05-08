/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.clientside;

import java.util.Set;
import java.util.UUID;

/**
 * Holds the state that is sent from the server side, state
 * includes a subset of member-uuids of a specific cluster.
 *
 * @param clusterUuid     uuid of the cluster connected by a client
 * @param members         single member-group as memberUuid-collection
 * @param version         version of members' list
 */
public record SubsetMembersView(UUID clusterUuid,
                                Set<UUID> members,
                                int version) {
}

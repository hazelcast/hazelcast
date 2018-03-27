/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum;

/**
 * Quorum service can be used to trigger cluster quorums at any time.
 * Normally quorums are done when any change happens on the member list.
 *
 * IMPORTANT: The term "quorum" simply refers to the count of members in the cluster required for an operation to succeed.
 * It does NOT refer to an implementation of Paxos or Raft protocols as used in many NoSQL and distributed systems.
 * The mechanism it provides in Hazelcast protects the user in case the number of nodes in a cluster drops below the
 * specified one.
 */
public interface QuorumService {

    /**
     * Returns the {@link Quorum} instance for a given quorum name.
     *
     * @param quorumName name of the quorum
     * @return {@link Quorum}
     * @throws IllegalArgumentException if no quorum found for given name
     * @throws NullPointerException     if quorumName is null
     */
    Quorum getQuorum(String quorumName) throws IllegalArgumentException;

    /**
     * Ensures that the quorum with the given name is present.
     * Throws a QuorumException if quorum not present.
     * Does not throw exception if quorumName null or quorum undefined.
     *
     * If (requiredQuorumPermissionType == READ) -> will check for presence of READ or READ_WRITE quorum<br/>
     * If (requiredQuorumPermissionType == WRITE) -> will check for presence of WRITE or READ_WRITE quorum<br/>
     * If (requiredQuorumPermissionType == READ_WRITE) -> will check for presence of READ_WRITE quorum<br/>
     *
     * @param quorumName                   quorum name to ensure, can be null or empty
     * @param requiredQuorumPermissionType type of quorum required
     * @throws QuorumException if quorum defined and not present
     */
    void ensureQuorumPresent(String quorumName, QuorumType requiredQuorumPermissionType) throws QuorumException;
}

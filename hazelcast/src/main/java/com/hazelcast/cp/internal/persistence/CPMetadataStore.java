/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.RaftGroupId;

import java.io.IOException;
import java.util.Collection;

/**
 * Persists and restores CP member metadata of the local member.
 */
public interface CPMetadataStore {

    /**
     * Returns true if this member is marked as AP member on the storage layer.
     * If {@code false} is returned, it means that AP/CP identity of the member
     * is not not known yet CP member discovery will run.
     */
    boolean isMarkedAPMember();

    /**
     *  Marks this member as AP member on the storage layer,
     *  if it is not a CP member already.
     * @return true if marked as AP, false otherwise
     */
    boolean tryMarkAPMember() throws IOException;

    /**
     * Returns true if this member has local member file persisted
     * on the storage layer.
     */
    boolean containsLocalMemberFile();

    /**
     * Persists {@link CPMember} identity of the local member to storage.
     */
    void persistLocalCPMember(CPMember member) throws IOException;

    /**
     * Reads {@link CPMember} identity of this member from storage.
     * If {@code null} is returned, it means that either local member is AP
     * or AP/CP identity of it is not known yet.
     */
    CPMember readLocalCPMember() throws IOException;

    /**
     * Persists active CP members list.
     * @param members CP members
     * @param commitIndex member list commit index
     */
    void persistActiveCPMembers(Collection<? extends CPMember> members, long commitIndex) throws IOException;

    /**
     * Reads active CP members and the commit index.
     * @param members collection to append read members
     * @return the member list commit index
     */
    long readActiveCPMembers(Collection<CPMember> members) throws IOException;

    /**
     * Persists group id of the METADATA group.
     */
    void persistMetadataGroupId(RaftGroupId groupId) throws IOException;

    /**
     * Reads group id of the METADATA group, or reads null if METADATA
     * group id has not changed
     */
    RaftGroupId readMetadataGroupId() throws IOException;
}

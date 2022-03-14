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
package com.hazelcast.client.impl.spi;

import com.hazelcast.cluster.Member;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;

public interface ClientMemberListProvider {

    void waitInitialMemberListFetched();

    /**
     * Gets the member with the given UUID.
     *
     * @param uuid The UUID of the member.
     * @return The member that was found, or null if not found. If UUID is null, null is returned.
     */
    Member getMember(@Nonnull UUID uuid);

    /**
     * Gets the collection of members.
     *
     * @return The collection of members. Null will never be returned.
     */
    Collection<Member> getMemberList();
}

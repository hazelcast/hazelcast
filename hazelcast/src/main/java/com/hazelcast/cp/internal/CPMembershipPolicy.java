/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPGroupId;

import java.util.Collection;

public interface CPMembershipPolicy {

    default CPMemberInfo toCPMemberInfo(Member member) {
        return new CPMemberInfo(member.getUuid(), member.getAddress(), false);
    }

    /**
     * Returns whether the given member set preserves the majority-of-leader-capable
     * invariant. OS implementation always returns true (no such constraint).
     */
    boolean isValidMembership(Collection<CPMemberInfo> members);

    /** Logs the skip of an unsafe substitute. No-op in OS. */
    void onRejectedSubstitute(CPMemberInfo substitute, CPMemberInfo leavingMember, CPGroupId groupId);

    /** Logs the rejection of discovery. No-op in OS. */
    void onRejectedDiscovery();

    /** Logs the skip of an unsafe addition. No-op in OS. */
    void onRejectedAddition(CPMemberInfo newMember, CPGroupId groupId);

    /** Whether two members are equivalent w.r.t. auto-step-down. Always true in OS. */
    boolean membersEquivalent(CPMemberInfo existing, CPMemberInfo verification);
}

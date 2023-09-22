/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.event.impl;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Published upon {@link com.hazelcast.cp.internal.RaftService#onShutdown(long, TimeUnit)}. This is a side effect of performing
 * a graceful shutdown.
 */
public class CPGroupAvailabilityEventGracefulImpl extends CPGroupAvailabilityEventImpl {
    public CPGroupAvailabilityEventGracefulImpl() {
    }

    public CPGroupAvailabilityEventGracefulImpl(CPGroupId groupId, Collection<CPMember> members,
                                                Collection<CPMember> unavailableMembers) {
        super(groupId, members, unavailableMembers);
    }

    @Override
    public int getClassId() {
        return CpEventDataSerializerHook.GROUP_AVAILABILITY_GRACEFUL_EVENT;
    }
}

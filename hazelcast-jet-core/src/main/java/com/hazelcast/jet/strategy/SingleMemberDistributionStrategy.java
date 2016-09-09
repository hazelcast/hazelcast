/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.strategy;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

/**
 * A shuffling strategy which shuffles all data to a single node, identified by the given address
 */
public class SingleMemberDistributionStrategy implements MemberDistributionStrategy {

    private final String host;
    private final int port;

    /**
     * Constructs the strategy with the given address
     */
    public SingleMemberDistributionStrategy(Member member) {
        host = member.getAddress().getHost();
        port = member.getAddress().getPort();

    }

    @Override
    public Collection<Member> getTargetMembers(JobContext jobContext) {
        try {
            MemberImpl member = new MemberImpl(new Address(host, port), false);
            return Collections.<Member>singletonList(member);
        } catch (UnknownHostException e) {
            throw unchecked(e);
        }
    }
}

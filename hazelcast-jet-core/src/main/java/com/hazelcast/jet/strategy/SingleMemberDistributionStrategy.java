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
import com.hazelcast.jet.JetDataSerializerHook;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * A distribution strategy which shuffles all data to a single node, identified by the given address
 */
public final class SingleMemberDistributionStrategy implements MemberDistributionStrategy, IdentifiedDataSerializable {

    private Member member;


    /**
     * Internal serialization use only
     */
    public SingleMemberDistributionStrategy() {
    }

    /**
     * Constructs the strategy with the given address
     */
    public SingleMemberDistributionStrategy(Member member) {
        this.member = member;
    }

    @Override
    public Set<Member> getTargetMembers(JobContext jobContext) {
        return singleton(member);
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetDataSerializerHook.SINGLE_MEMBER_DISTRIBUTION_STRATEGY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(member);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = in.readObject();
    }
}


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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.MemberImplConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberImplConstructorTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        HazelcastInstance hz = createHazelcastInstance();
        MemberImpl memberImpl = (MemberImpl) hz.getCluster().getLocalMember();

        MemberImplConstructor constructor = new MemberImplConstructor(MemberImpl.class);
        MemberImpl clonedMemberImpl = (MemberImpl) constructor.createNew(memberImpl);

        assertEquals(memberImpl.localMember(), clonedMemberImpl.localMember());
        assertEquals(memberImpl.isLiteMember(), clonedMemberImpl.isLiteMember());
        assertEquals(memberImpl.getAddress(), clonedMemberImpl.getAddress());
        assertEquals(memberImpl.getSocketAddress(), clonedMemberImpl.getSocketAddress());
        assertEquals(memberImpl.getUuid(), clonedMemberImpl.getUuid());
        assertEquals(memberImpl.getAttributes(), clonedMemberImpl.getAttributes());
        assertEquals(memberImpl.getVersion(), clonedMemberImpl.getVersion());
    }
}

/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryUtilsTest extends SqlTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testVersionMismatch() {
        HazelcastInstance member = factory.newHazelcastInstance();

        NodeEngine nodeEngine = nodeEngine(member);
        String memberId = nodeEngine.getLocalMember().getUuid().toString();
        String memberVersion = nodeEngine.getLocalMember().getVersion().toString();

        try {
            QueryUtils.createPartitionMap(nodeEngine, new MemberVersion(0, 0, 0));

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.GENERIC, e.getCode());
            assertEquals("Cannot execute SQL query when members have different versions (make sure that all members "
                + "have the same version) {localMemberId=" + memberId + ", localMemberVersion=0.0.0, remoteMemberId="
                + memberId + ", remoteMemberVersion=" + memberVersion + "}", e.getMessage());
        }
    }
}

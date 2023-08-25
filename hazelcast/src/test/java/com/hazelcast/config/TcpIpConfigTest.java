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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TcpIpConfigTest {

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(TcpIpConfig.class)
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }

    @Test
    public void testSetMembers_whenMembersSeparatedByDelimiter_AllMembersShouldBeAddedToMemberList() {
        TcpIpConfig tcpIpConfig = new TcpIpConfig();
        List<String> expectedMemberAddresses =
                Arrays.asList("10.11.12.1", "10.11.12.2", "10.11.12.3:5803", "10.11.12.4", "10.11.12.5", "10.11.12.6");

        List<String> memberAddresses =
                Arrays.asList(" 10.11.12.1, 10.11.12.2", " 10.11.12.3:5803 ;; 10.11.12.4, 10.11.12.5  10.11.12.6");
        tcpIpConfig.setMembers(memberAddresses);
        assertEquals(expectedMemberAddresses, tcpIpConfig.getMembers());
    }

    @Test
    public void testAddMember_whenMembersSeparatedByDelimiter_AllMembersShouldBeAddedToMemberList() {
        TcpIpConfig tcpIpConfig = new TcpIpConfig();
        List<String> expectedMemberAddresses =
                Arrays.asList("10.11.12.1", "10.11.12.2", "10.11.12.3", "10.11.12.4", "10.11.12.5", "10.11.12.6", "localhost:8803");

        String members1 = " 10.11.12.1,; 10.11.12.2";
        String members2 = " 10.11.12.3 ;; 10.11.12.4 , 10.11.12.5  10.11.12.6   , localhost:8803";
        tcpIpConfig.addMember(members1);
        tcpIpConfig.addMember(members2);

        assertEquals(expectedMemberAddresses, tcpIpConfig.getMembers());
    }
}

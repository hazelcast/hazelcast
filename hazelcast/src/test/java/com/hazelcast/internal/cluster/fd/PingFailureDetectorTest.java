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

package com.hazelcast.internal.cluster.fd;

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PingFailureDetectorTest {

    private PingFailureDetector<Member> failureDetector;

    @Before
    public void setup() {
        failureDetector = new PingFailureDetector<>(3);
    }

    @Test
    public void member_isNotAlive_whenNoHeartbeat() {
        Member member = newMember(5000);
        assertFalse(failureDetector.isAlive(member));
    }

    @Test
    public void member_isNotAlive_afterThreeAttempts() {
        Member member = newMember(5000);
        failureDetector.logAttempt(member);
        failureDetector.logAttempt(member);
        failureDetector.logAttempt(member);
        assertFalse(failureDetector.isAlive(member));
    }

    @Test
    public void member_isAlive_afterThreeAttempts_afterHeartbeat() {
        Member member = newMember(5000);
        failureDetector.logAttempt(member);
        failureDetector.logAttempt(member);
        failureDetector.logAttempt(member);
        failureDetector.heartbeat(member);
        assertTrue(failureDetector.isAlive(member));
    }

    @Test
    public void member_isAlive_whenHeartbeat() {
        Member member = newMember(5000);
        failureDetector.heartbeat(member);
        assertTrue(failureDetector.isAlive(member));
    }

    @Test
    public void member_isAlive_beforeHeartbeatTimeout() {
        Member member = newMember(5000);
        failureDetector.heartbeat(member);
        assertTrue(failureDetector.isAlive(member));
    }

    @Test
    public void remove_whenNoHeartbeat() {
        Member member = newMember(5000);
        failureDetector.remove(member);
        assertFalse(failureDetector.isAlive(member));
    }

    @Test
    public void remove_afterHeartbeat() {
        Member member = newMember(5000);
        failureDetector.heartbeat(member);

        failureDetector.remove(member);
        assertFalse(failureDetector.isAlive(member));
    }

    @Test
    public void reset_whenNoHeartbeat() {
        Member member = newMember(5000);
        failureDetector.reset();
        assertFalse(failureDetector.isAlive(member));
    }

    @Test
    public void reset_afterHeartbeat() {
        Member member = newMember(5000);
        failureDetector.heartbeat(member);

        failureDetector.reset();
        assertFalse(failureDetector.isAlive(member));
    }

    private static Member newMember(int port) {
        MemberVersion memberVersion = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
        return new MemberImpl.Builder(newAddress(port))
                .version(memberVersion)
                .uuid(newUnsecureUUID())
                .build();
    }

    private static Address newAddress(int port) {
        try {
            return new Address(InetAddress.getLocalHost(), port);
        } catch (UnknownHostException e) {
            fail("Could not create new Address: " + e.getMessage());
        }
        return null;
    }
}

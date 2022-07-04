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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterFailureDetectorTest {

    private static final long HEARTBEAT_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

    @Parameterized.Parameters(name = "fd:{0}")
    public static Collection<ClusterFailureDetectorType> parameters() {
        return Arrays.asList(ClusterFailureDetectorType.values());
    }

    @Parameterized.Parameter
    public ClusterFailureDetectorType failureDetectorType;

    private ClusterFailureDetector failureDetector;

    @Before
    public void setup() {
        switch (failureDetectorType) {
            case DEADLINE:
                failureDetector = new DeadlineClusterFailureDetector(HEARTBEAT_TIMEOUT);
                break;
            case PHI_ACCRUAL:
                failureDetector
                        = new PhiAccrualClusterFailureDetector(HEARTBEAT_TIMEOUT, 1, new HazelcastProperties(new Properties()));
                break;
            default:
                throw new IllegalArgumentException(failureDetectorType.toString());
        }
    }

    @Test
    public void member_isNotAlive_whenNoHeartbeat() {
        Member member = newMember(5000);
        assertFalse(failureDetector.isAlive(member, Clock.currentTimeMillis()));
    }

    @Test
    public void member_isAlive_whenHeartbeat() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);
        assertTrue(failureDetector.isAlive(member, timestamp));
    }

    @Test
    public void member_isAlive_beforeHeartbeatTimeout() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);
        assertTrue(failureDetector.isAlive(member, timestamp + HEARTBEAT_TIMEOUT / 2));
    }

    @Test
    public void member_isNotAlive_afterHeartbeatTimeout() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);

        long ts = timestamp + HEARTBEAT_TIMEOUT * 2;
        assertFalse("Suspicion level: " + failureDetector.suspicionLevel(member, ts), failureDetector.isAlive(member, ts));
    }

    @Test
    public void lastHeartbeat_whenNoHeartbeat() {
        Member member = newMember(5000);
        long lastHeartbeat = failureDetector.lastHeartbeat(member);
        assertEquals(0L, lastHeartbeat);
    }

    @Test
    public void lastHeartbeat() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);

        long lastHeartbeat = failureDetector.lastHeartbeat(member);
        assertEquals(timestamp, lastHeartbeat);
    }

    @Test
    public void suspicionLevel_whenNoHeartbeat() {
        Member member = newMember(5000);
        double suspicionLevel = failureDetector.suspicionLevel(member, Clock.currentTimeMillis());

        double failureLevel = getFailureSuspicionLevel(failureDetector);
        assertEquals(failureLevel, suspicionLevel, 0d);
    }

    @Test
    public void suspicionLevel_whenHeartbeat() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);

        double suspicionLevel = failureDetector.suspicionLevel(member, timestamp);
        assertEquals(0, suspicionLevel, 0d);
    }

    @Test
    public void suspicionLevel_beforeHeartbeatTimeout() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);

        double suspicionLevel = failureDetector.suspicionLevel(member, timestamp + HEARTBEAT_TIMEOUT / 2);

        double failureLevel = getFailureSuspicionLevel(failureDetector);
        assertThat(suspicionLevel, lessThan(failureLevel));
    }

    @Test
    public void suspicionLevel_afterHeartbeatTimeout() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);

        double suspicionLevel = failureDetector.suspicionLevel(member, timestamp + HEARTBEAT_TIMEOUT * 2);

        double failureLevel = getFailureSuspicionLevel(failureDetector);
        assertThat(suspicionLevel, greaterThanOrEqualTo(failureLevel));
    }

    private static double getFailureSuspicionLevel(ClusterFailureDetector failureDetector) {
        if (failureDetector instanceof DeadlineClusterFailureDetector) {
            return 1;
        }
        if (failureDetector instanceof PhiAccrualClusterFailureDetector) {
            return Integer.parseInt(PhiAccrualClusterFailureDetector.HEARTBEAT_PHI_FAILURE_DETECTOR_THRESHOLD.getDefaultValue());
        }
        throw new IllegalArgumentException();
    }

    @Test
    public void remove_whenNoHeartbeat() {
        Member member = newMember(5000);
        failureDetector.remove(member);
        assertFalse(failureDetector.isAlive(member, Clock.currentTimeMillis()));
    }

    @Test
    public void remove_afterHeartbeat() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);

        failureDetector.remove(member);
        assertFalse(failureDetector.isAlive(member, Clock.currentTimeMillis()));
    }

    @Test
    public void reset_whenNoHeartbeat() {
        Member member = newMember(5000);
        failureDetector.reset();
        assertFalse(failureDetector.isAlive(member, Clock.currentTimeMillis()));
    }

    @Test
    public void reset_afterHeartbeat() {
        Member member = newMember(5000);
        long timestamp = Clock.currentTimeMillis();
        failureDetector.heartbeat(member, timestamp);

        failureDetector.reset();
        assertFalse(failureDetector.isAlive(member, Clock.currentTimeMillis()));
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

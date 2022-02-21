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

package com.hazelcast.cp;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPConfigCheckerTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void whenGroupSize_smallerThanMin() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setGroupSize(CPSubsystemConfig.MIN_GROUP_SIZE - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenGroupSize_greaterThanMax() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setGroupSize(CPSubsystemConfig.MAX_GROUP_SIZE + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenGroupSize_even() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setGroupSize(4);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenMemberCount_smallerThanMin() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(CPSubsystemConfig.MIN_GROUP_SIZE - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenGroupSize_greaterThanMemberCount() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(3);
        cpSubsystemConfig.setGroupSize(5);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSessionTTL_lessThanHeartbeatInterval() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setSessionTimeToLiveSeconds(5);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(10);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSessionTTL_greaterThanMissingMemberTimeout() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setSessionTimeToLiveSeconds(100);
        cpSubsystemConfig.setMissingCPMemberAutoRemovalSeconds(10);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenPersistenceEnabled_andCPSubsystemNotEnabled() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setPersistenceEnabled(true);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }
}

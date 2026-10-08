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

package com.hazelcast.cp;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPConfigCheckerTest extends HazelcastTestSupport {

    private static final String ENTERPRISE_JARS_HINT =
            " Make sure you have Hazelcast Enterprise JARs on your classpath!";

    private static final String AUTO_STEP_DOWN_MESSAGE =
            "Leader Auto Step Down is supported in Hazelcast Enterprise only." + ENTERPRISE_JARS_HINT;

    private static final String CP_MEMBER_PRIORITY_MESSAGE =
            "CP member priority is supported in Hazelcast Enterprise only." + ENTERPRISE_JARS_HINT;

    private static final String CP_PERSISTENCE_MESSAGE =
            "CP Persistence is supported in Hazelcast Enterprise only." + ENTERPRISE_JARS_HINT;

    private static final String CP_MAP_PURGE_MESSAGE =
            "CPMap purge is supported in Hazelcast Enterprise only." + ENTERPRISE_JARS_HINT;

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

    // enterprise-only feature checks

    @Test
    public void whenDefaultConfig_andNotEnterprise_thenNoEnterpriseFeatureIsRejected() {
        assumeFalse(getBuildInfo().isEnterprise());

        checkCPSubsystemConfig(enabledCPSubsystemConfig());
    }

    @Test
    public void whenAutoStepDownWhenLeaderEnabled_andNotEnterprise() {
        assumeFalse(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.setAutoStepDownWhenLeader(true);

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class,
                () -> checkCPSubsystemConfig(cpSubsystemConfig));
        assertEquals(AUTO_STEP_DOWN_MESSAGE, e.getMessage());
    }

    @Test
    public void whenAutoStepDownWhenLeaderEnabled_andEnterprise() {
        assumeTrue(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.setAutoStepDownWhenLeader(true);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test
    public void whenCPMemberPriorityIsPositive_andNotEnterprise() {
        assumeFalse(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.setCPMemberPriority(1);

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class,
                () -> checkCPSubsystemConfig(cpSubsystemConfig));
        assertEquals(CP_MEMBER_PRIORITY_MESSAGE, e.getMessage());
    }

    @Test
    public void whenCPMemberPriorityIsDefault_andNotEnterprise() {
        assumeFalse(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.setCPMemberPriority(0);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test
    public void whenCPMemberPriorityIsPositive_andEnterprise() {
        assumeTrue(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.setCPMemberPriority(1);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test
    public void whenPersistenceEnabled_andNotEnterprise() {
        assumeFalse(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.setPersistenceEnabled(true);

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class,
                () -> checkCPSubsystemConfig(cpSubsystemConfig));
        assertEquals(CP_PERSISTENCE_MESSAGE, e.getMessage());
    }

    @Test
    public void whenPersistenceEnabled_andEnterprise() {
        assumeTrue(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.setPersistenceEnabled(true);

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test
    public void whenCPMapPurgeEnabled_andNotEnterprise() {
        assumeFalse(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.addCPMapConfig(purgingCPMapConfig("purging-map"));

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class,
                () -> checkCPSubsystemConfig(cpSubsystemConfig));
        assertEquals(CP_MAP_PURGE_MESSAGE, e.getMessage());
    }

    @Test
    public void whenOneOfSeveralCPMapsHasPurgeEnabled_andNotEnterprise() {
        assumeFalse(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.addCPMapConfig(new CPMapConfig("plain-map"));
        cpSubsystemConfig.addCPMapConfig(purgingCPMapConfig("purging-map"));

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class,
                () -> checkCPSubsystemConfig(cpSubsystemConfig));
        assertEquals(CP_MAP_PURGE_MESSAGE, e.getMessage());
    }

    @Test
    public void whenCPMapPurgeDisabled_andNotEnterprise() {
        assumeFalse(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.addCPMapConfig(new CPMapConfig("plain-map"));

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    @Test
    public void whenCPMapPurgeEnabled_andEnterprise() {
        assumeTrue(getBuildInfo().isEnterprise());

        CPSubsystemConfig cpSubsystemConfig = enabledCPSubsystemConfig();
        cpSubsystemConfig.addCPMapConfig(purgingCPMapConfig("purging-map"));

        checkCPSubsystemConfig(cpSubsystemConfig);
    }

    private static CPSubsystemConfig enabledCPSubsystemConfig() {
        CPSubsystemConfig cpSubsystemConfig = new Config().getCPSubsystemConfig();
        cpSubsystemConfig.setCPMemberCount(3);
        return cpSubsystemConfig;
    }

    private static CPMapConfig purgingCPMapConfig(String name) {
        CPMapConfig cpMapConfig = new CPMapConfig(name);
        cpMapConfig.setPurgeEnabled(true);
        return cpMapConfig;
    }
}

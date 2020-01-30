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

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static com.hazelcast.jet.impl.config.JetDeclarativeConfigUtil.SYSPROP_JET_CONFIG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class})
public class JetConfigMixAndMatchTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    @After
    public void beforeAndAfter() {
        System.clearProperty(SYSPROP_JET_CONFIG);
        System.clearProperty(SYSPROP_MEMBER_CONFIG);
    }


    @Test
    public void when_XmlJetConfig_and_XmlMemberConfigSet_thenLoadsConfigCorrectly() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:hazelcast-jet-test.xml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:hazelcast-jet-member-test.xml");

        // When
        JetConfig jetConfig = JetConfig.loadDefault();
        Config hzConfig = jetConfig.getHazelcastConfig();

        // Then
        assertConfigs(jetConfig, hzConfig);
    }

    @Test
    public void when_YamlJetConfig_and_YamlMemberConfigSet_thenLoadsConfigCorrectly() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:hazelcast-jet-test.yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:hazelcast-jet-member-test.yaml");

        // When
        JetConfig jetConfig = JetConfig.loadDefault();
        Config hzConfig = jetConfig.getHazelcastConfig();

        // Then
        assertConfigs(jetConfig, hzConfig);
    }

    @Test
    public void when_XmlJetConfig_and_YamlMemberConfigSet_thenLoadsConfigCorrectly() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:hazelcast-jet-test.xml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:hazelcast-jet-member-test.yaml");

        // When
        JetConfig jetConfig = JetConfig.loadDefault();
        Config hzConfig = jetConfig.getHazelcastConfig();

        // Then
        assertConfigs(jetConfig, hzConfig);
    }

    @Test
    public void when_YamlJetConfig_and_XmlMemberConfigSet_thenLoadsConfigCorrectly() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:hazelcast-jet-test.yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:hazelcast-jet-member-test.xml");

        // When
        JetConfig jetConfig = JetConfig.loadDefault();
        Config hzConfig = jetConfig.getHazelcastConfig();

        // Then
        assertConfigs(jetConfig, hzConfig);
    }

    private void assertConfigs(JetConfig jetConfig, Config hzConfig) {
        assertThat(jetConfig, not(nullValue()));
        assertEquals("cooperativeThreadCount", 55, jetConfig.getInstanceConfig().getCooperativeThreadCount());
        assertEquals("backupCount", 2, jetConfig.getInstanceConfig().getBackupCount());
        assertEquals("flowControlMs", 50, jetConfig.getInstanceConfig().getFlowControlPeriodMs());
        assertEquals("scaleUpDelayMillis", 1234, jetConfig.getInstanceConfig().getScaleUpDelayMillis());
        assertFalse("losslessRestartEnabled", jetConfig.getInstanceConfig().isLosslessRestartEnabled());
        assertThat(hzConfig, not(nullValue()));
        assertThat(hzConfig.getClusterName(), equalTo("imdg"));
    }

}

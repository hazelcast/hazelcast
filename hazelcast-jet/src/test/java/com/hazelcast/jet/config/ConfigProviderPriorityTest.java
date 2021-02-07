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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.jet.impl.config.ConfigProvider;
import com.hazelcast.jet.test.JetDeclarativeConfigFileHelper;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class})
public class ConfigProviderPriorityTest {

    private JetDeclarativeConfigFileHelper helper = new JetDeclarativeConfigFileHelper();

    @Before
    @After
    public void tearDown() throws Exception {
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void when_loadingJetConfigFromWorkDir_yamlAndXmlFilesArePresent_then_pickYaml() throws Exception {
        helper.givenXmlJetConfigFileOnWorkdir("hazelcast-jet.xml", "foo", "bar-xml");
        helper.givenYamlJetConfigFileOnWorkdir("hazelcast-jet.yaml", "foo", "bar-yaml");

        JetConfig jetConfig = ConfigProvider.locateAndGetJetConfig();
        Assert.assertEquals("bar-yaml", jetConfig.getProperties().getProperty("foo"));
    }

    @Test
    public void when_loadingClientConfigFromWorkDir_yamlAndXmlFilesArePresent_then_pickYaml() throws Exception {
        helper.givenXmlClientConfigFileInWorkDir("instance-xml");
        helper.givenYamlClientConfigFileInWorkDir("instance-yaml");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        Assert.assertEquals("instance-yaml", clientConfig.getInstanceName());
    }

    @Test
    public void when_loadingMemberConfigFromWorkDir_yamlAndXmlFilesArePresent_then_pickYaml() throws Exception {
        helper.givenXmlConfigFileInWorkDir("instance-xml");
        helper.givenYamlConfigFileInWorkDir("instance-yaml");

        Config config = ConfigProvider.locateAndGetMemberConfig(null);
        Assert.assertEquals("instance-yaml", config.getInstanceName());
    }
}

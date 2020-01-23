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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManagementCenterConfigTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Test
    public void testScriptingDisabledByDefault() {
        HazelcastInstance hz = factory.newHazelcastInstance();
        Assert.assertFalse(hz.getConfig().getManagementCenterConfig().isScriptingEnabled());
    }

    @Test
    public void testScriptingDisabled_whenProgrammaticConfig() {
        HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        Assert.assertFalse(hz.getConfig().getManagementCenterConfig().isScriptingEnabled());
    }

    @Test
    public void testScriptingDisabled_whenXmlConfig() {
        InMemoryXmlConfig xmlConfig = new InMemoryXmlConfig("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n"
                + "<management-center />\n"
                + "</hazelcast>");
        HazelcastInstance hz = factory.newHazelcastInstance(xmlConfig);
        Assert.assertFalse(hz.getConfig().getManagementCenterConfig().isScriptingEnabled());
    }

    @Test
    public void testScriptingDisabled_whenYamlConfig() {
        InMemoryYamlConfig yamlConfig = new InMemoryYamlConfig("hazelcast:\n  cluster-name: name\n");
        HazelcastInstance hz = factory.newHazelcastInstance(yamlConfig);
        Assert.assertFalse(hz.getConfig().getManagementCenterConfig().isScriptingEnabled());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(ManagementCenterConfig.class)
                      .allFieldsShouldBeUsed()
                      .suppress(Warning.NONFINAL_FIELDS)
                      .verify();
    }
}

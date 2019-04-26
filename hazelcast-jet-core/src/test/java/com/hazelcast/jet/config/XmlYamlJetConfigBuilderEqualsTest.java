/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class XmlYamlJetConfigBuilderEqualsTest {

    @Test
    public void testDefaultConfig() {
        //Given
        JetConfig xmlConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), "hazelcast-jet-default.xml");
        JetConfig yamlConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), "hazelcast-jet-default.yaml");

        //When
        String xmlConfigFromXml = JetConfigXmlGenerator.generate(xmlConfig);
        String xmlConfigFromYaml = JetConfigXmlGenerator.generate(yamlConfig);

        //Then
        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    @Test
    public void testFullConfig() {
        //Given
        JetConfig xmlConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), "hazelcast-jet-full-example.xml");
        JetConfig yamlConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), "hazelcast-jet-full-example.yaml");

        //When
        String xmlConfigFromXml = JetConfigXmlGenerator.generate(xmlConfig);
        String xmlConfigFromYaml = JetConfigXmlGenerator.generate(yamlConfig);

        //Then
        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }
}

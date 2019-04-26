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

import com.hazelcast.config.InvalidConfigurationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Abstract class defining the common test cases for XML and YAML
 * based configuration tests.
 * <p/>
 * All common test cases should be defined in this class to guarantee
 * compilation error if either YAML or XML configuration misses to cover
 * a common case.
 * <p/>
 *
 * @see XmlJetConfigBuilderTest
 * @see YamlJetConfigBuilderTest
 */
public abstract class AbstractJetConfigBuilderTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public abstract void testConfiguration_withNullInputStream();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testInvalidRootElement();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testHazelcastJetTagAppearsTwice();

    @Test
    public abstract void readMetricsConfig();

    @Test
    public abstract void readInstanceConfig();

    @Test
    public abstract void readEdgeDefaults();

    @Test
    public abstract void readProperties();

    protected abstract JetConfig buildConfig(String configuration);


}

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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * Abstract test class defining the common test cases for loading XML and YAML
 * based member configuration file from system properties.
 *
 * @see XmlJetConfigWithSystemPropertyTest
 * @see YamlJetConfigWithSystemPropertyTest
 */
@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractJetMemberConfigWithSystemPropertyTest extends AbstractJetConfigWithSystemPropertyTest {

    @Test(expected = HazelcastException.class)
    public abstract void when_filePathMemberSpecifiedNonExistingFile_thenThrowsException() throws Exception;

    @Test
    public abstract void when_filePathMemberSpecified_usesSpecifiedFile() throws IOException;

    @Test(expected = HazelcastException.class)
    public abstract void when_classpathMemberSpecifiedNonExistingFile_thenThrowsException();

    @Test
    public abstract void when_classpathMemberSpecified_usesSpecifiedResource();

    @Test
    public abstract void when_configMemberHasVariable_variablesAreReplaced();

    @Test
    public abstract void when_edgeDefaultsSpecified_usesSpecified();


}

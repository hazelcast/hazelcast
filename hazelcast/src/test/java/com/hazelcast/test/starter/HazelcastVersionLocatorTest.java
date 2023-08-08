/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.HazelcastVersionLocator.Artifact;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class HazelcastVersionLocatorTest extends HazelcastTestSupport {
    @Test
    public void testLocateVersion() {
        final Map<Artifact, File> map = HazelcastVersionLocator.locateVersion("5.0", false);

        assertNotNull(map);

        File jar = map.get(Artifact.OS_JAR);

        assertNotNull(jar);

        final String repoDirectory = System.getProperty("user.home") + File.separator + ".m2" + File.separator + "repository"
                + File.separator + "com" + File.separator + "hazelcast" + File.separator + "hazelcast" + File.separator + "5.0"
                + File.separator;

        assertEquals(repoDirectory + "hazelcast-5.0.jar", jar.toString());

        jar = map.get(Artifact.OS_TEST_JAR);

        assertNotNull(jar);

        assertEquals(repoDirectory + "hazelcast-5.0-tests.jar", jar.toString());
    }
}
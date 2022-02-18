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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SymmetricEncryptionConfigTest extends HazelcastTestSupport {

    private SymmetricEncryptionConfig config = new SymmetricEncryptionConfig();

    @Test
    public void testSetEnabled() {
        config.setEnabled(true);

        assertTrue(config.isEnabled());
    }

    @Test
    public void testSetAlgorithm() {
        config.setAlgorithm("myAlgorithm");

        assertEquals("myAlgorithm", config.getAlgorithm());
    }

    @Test
    public void testSetPassword() {
        config.setPassword("myPassword");

        assertEquals("myPassword", config.getPassword());
    }

    @Test
    public void testSetSalt() {
        config.setSalt("mySalt");

        assertEquals("mySalt", config.getSalt());
    }

    @Test
    public void testSetIterationCount() {
        config.setIterationCount(23);

        assertEquals(23, config.getIterationCount());
    }

    @Test
    public void testSetKey() {
        byte[] key = new byte[]{23, 42};
        config.setKey(key);

        assertEquals(key[0], config.getKey()[0]);
        assertEquals(key[1], config.getKey()[1]);
    }

    @Test
    public void testToString() {
        assertContains(config.toString(), "SymmetricEncryptionConfig");
    }
}

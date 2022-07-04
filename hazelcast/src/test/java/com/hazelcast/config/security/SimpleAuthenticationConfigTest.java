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

package com.hazelcast.config.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.security.SimpleAuthenticationConfig.UserDto;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class SimpleAuthenticationConfigTest {

    @Test
    public void testAddUserAtMostOnce() {
        SimpleAuthenticationConfig c = new SimpleAuthenticationConfig();
        c.addUser("user", "password");
        assertThrows(IllegalArgumentException.class, () -> c.addUser("user", "password2"));
        assertThrows(IllegalArgumentException.class, () -> c.addUser("user", new UserDto("password3")));
        assertTrue(c.getUsernames().contains("user"));
    }

    @Test
    public void testAddUser() {
        SimpleAuthenticationConfig c = new SimpleAuthenticationConfig();
        c.addUser("user1", "password1");
        c.addUser("user2", "password2", "role1");
        c.addUser("user3", "password3", "role1", "role2", "role3");
        assertEquals(3, c.getUsernames().size());
        assertEquals("password1", c.getPassword("user1"));
        assertTrue(c.getRoles("user3").contains("role3"));
    }

    @Test
    public void testAddUserWithEmptyPassword() {
        SimpleAuthenticationConfig c = new SimpleAuthenticationConfig();
        assertThrows(IllegalArgumentException.class, () -> c.addUser("user", ""));
        assertThrows(IllegalArgumentException.class, () -> c.addUser("user", (String) null));
    }

    @Test
    public void testRoleSeparator() {
        SimpleAuthenticationConfig c = new SimpleAuthenticationConfig();
        assertThrows(IllegalArgumentException.class, () -> c.setRoleSeparator(""));
        c.setRoleSeparator(":");
        assertEquals(":", c.getRoleSeparator());
        c.setRoleSeparator(null);
        assertNull(c.getRoleSeparator());
    }
}

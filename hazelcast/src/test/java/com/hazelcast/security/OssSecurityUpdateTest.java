/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collections;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OssSecurityUpdateTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testEnable_atRuntime() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz = factory.newHazelcastInstance();
        SecurityConfig securityConfig = hz.getConfig().getSecurityConfig();

        exception.expect(UnsupportedOperationException.class);
        securityConfig.setEnabled(true);
    }

    @Test
    public void testUpdate_whenSecurityNotEnabled() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz = factory.newHazelcastInstance();
        SecurityConfig securityConfig = hz.getConfig().getSecurityConfig();

        exception.expect(UnsupportedOperationException.class);
        securityConfig.setClientPermissionConfigs(Collections.<PermissionConfig>emptySet());
    }
}

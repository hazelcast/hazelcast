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

import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Locale;

import static org.junit.Assert.assertSame;

/**
 * Tests for {@link LoginModuleConfig} class.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoginModuleConfigTest {

    @Test
    public void testLoginModuleUsageSelection() {
        Locale locale = Locale.getDefault();
        try {
            assertSame(LoginModuleUsage.OPTIONAL, LoginModuleUsage.get("optional"));
            Locale.setDefault(new Locale("tr"));
            assertSame(LoginModuleUsage.OPTIONAL, LoginModuleUsage.get("optional"));
        } finally {
            Locale.setDefault(locale);
        }
    }
}

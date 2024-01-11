/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class UserCodeNamespaceServiceLicenseTest {

    @Test
    public void testNamespaceServiceOnlyAvailableFromEE() {
        try {
            Config config = smallInstanceConfigWithoutJetAndMetrics();
            config.getNamespacesConfig().setEnabled(true);
            config.getNamespacesConfig().addNamespaceConfig(new UserCodeNamespaceConfig(randomString()));
            Hazelcast.newHazelcastInstance(config);
            throw new AssertionError("NamespaceService was created from OS Edition");
        } catch (IllegalStateException ex) {
            assertTrue("NamespaceService creation does not fail with expected IllegalStateException from OS",
                    ex.getMessage().contains("User Code Namespaces requires Hazelcast Enterprise Edition"));
        }
    }
}

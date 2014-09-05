/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author mdogan 5/28/13
 */

@Ignore//TODO
@RunWith(Categories.class)
@Categories.IncludeCategory(ClientCompatibleTest.class)
@Suite.SuiteClasses({})
public class ClientCompatibleTestsSuit {

    @BeforeClass
    public static void setUp() {
        System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "true");
        System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_CLIENT, "true");

        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }
}

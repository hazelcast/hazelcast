/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.GetCPObjectInfosTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClientGetCPObjectInfosTest extends GetCPObjectInfosTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        GetCPObjectInfosTest.beforeClass();
        cp = client.getCPSubsystem();
    }

    @Test
    public void theCPGroupDestroyedExceptionShouldContainGroupId() {
        IAtomicLong counter = cp.getAtomicLong("counter@my_group_to_destroy");
        counter.set(0);
        CPGroupId myGroup = groupId("my_group_to_destroy");
        instances[0].getCPSubsystem()
                .getCPSubsystemManagementService()
                .forceDestroyCPGroup("my_group_to_destroy")
                .toCompletableFuture()
                .join();

        assertThatThrownBy(counter::get)
                .hasStackTraceContaining(myGroup.toString());
    }
}

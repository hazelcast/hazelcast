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

package com.hazelcast.security.permission;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.Permission;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class UserCodeDeploymentPermissionTest extends PermissionTestSupport {

    @Override
    protected Permission createPermission(String name, String... actions) {
        return new UserCodeDeploymentPermission(actions);
    }

    @Test
    public void checkDeployPermission_whenAll() {
        new CheckPermission().of("deploy").against("deploy").expect(true).run();
    }

    @Test
    public void checkDeployPermission() {
        new CheckPermission().of("deploy").against("all").expect(true).run();
    }

    @Test
    public void checkAllPermission_whenDeploy() {
        new CheckPermission().of("all").against("deploy").expect(true).run();
    }

}

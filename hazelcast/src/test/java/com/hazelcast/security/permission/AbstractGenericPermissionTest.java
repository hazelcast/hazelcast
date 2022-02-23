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

import org.junit.Test;

/**
 * Abstract Permission Tests.
 */
public abstract class AbstractGenericPermissionTest
        extends PermissionTestSupport {

    @Test
    public void checkReadPermission() {
        new CheckPermission().of("read").against("read").expect(true).run();
    }

    @Test
    public void checkReadPermission_whenAll() {
        new CheckPermission().of("read").against("all").expect(true).run();
    }

    @Test
    public void checkReadPermission_whenOnlyCreateAllowed() {
        new CheckPermission().of("read").against("create").expect(false).run();
    }


    @Test
    public void checkModifyPermission() {
        new CheckPermission().of("modify").against("modify").expect(true).run();
    }

    @Test
    public void checkModifyPermission_whenAll() {
        new CheckPermission().of("modify").against("all").expect(true).run();
    }

    @Test
    public void checkModifyPermission_whenOnlyReadAllowed() {
        new CheckPermission().of("modify").against("read").expect(false).run();
    }

    @Test
    public void checkModifyPermission_whenOnlyReadAndCreateAllowed() {
        new CheckPermission().of("modify").against("read", "create").expect(false).run();
    }

    @Test
    public void checkModifyPermission_whenOnlyReadCreateAndDeleteAllowed() {
        new CheckPermission().of("modify").against("read", "create", "delete").expect(false).run();
    }

    @Test
    public void checkCreatePermission() {
        new CheckPermission().of("create").against("create").expect(true).run();
    }


    @Test
    public void checkCreatePermission_whenAll() {
        new CheckPermission().of("create").against("all").expect(true).run();
    }

    @Test
    public void checkCreatePermission_whenOnlyReadAllowed() {
        new CheckPermission().of("create").against("read").expect(false).run();
    }

    @Test
    public void checkDestroyPermission() {
        new CheckPermission().of("destroy").against("destroy").expect(true).run();
    }

    @Test
    public void checkDestroyPermission_whenAll() {
        new CheckPermission().of("destroy").against("all").expect(true).run();
    }


    @Test
    public void checkDestroyPermission_whenOnlyReadAllowed() {
        new CheckPermission().of("destroy").against("read").expect(false).run();
    }

}

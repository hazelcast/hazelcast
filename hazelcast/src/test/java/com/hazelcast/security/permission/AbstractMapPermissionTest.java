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
 * Abstract Map Permission Tests.
 */
public abstract class AbstractMapPermissionTest
        extends PermissionTestSupport {

    protected abstract String[] getActions();

    @Test
    public void willReturnFalseForNoPermOnPut() {
        new CheckPermission().of("put").against("read", "create").expect(false).run();
    }

    @Test
    public void willReturnFalseForNoPermOnListen() {
        new CheckPermission().of("listen").against("read", "create", "put").expect(false).run();
    }

    @Test
    public void willReturnTrueForPermOnPutOn() {
        new CheckPermission().of("put").against("put", "read", "create").expect(true).run();
    }

    @Test
    public void willReturnTrueForPermOnAll() {
        new CheckPermission().of("put").against("all").expect(true).run();
    }

    @Test
    public void willReturnTrueWhenNameUseMatchingWildcard() {
        new CheckPermission()
                .withAllowedName("myDataStructure.*")
                .withRequestedName("myDataStructure.foo")
                .of("put")
                .against(getActions())
                .expect(true).run();
    }

    @Test
    public void willReturnFalseWhenNameUseNonNames() {
        new CheckPermission()
                .withAllowedName("myDataStructure")
                .withRequestedName("myOtherDataStructure")
                .of("put")
                .against(getActions())
                .expect(false).run();
    }


}

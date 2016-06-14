package com.hazelcast.security.permission;

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.Test;

import java.security.Permission;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Abstract Permission Tests. A small selection of combinations are used here, use an array permutation algorithm.
 * <p/>
 * Could use <a href="http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/Collections2.html#permutations(java.util.Collection)">Google Guava Permutations</a>
 */
public abstract class AbstractPermissionTest {

    protected abstract String[] getActions();

    protected abstract Permission createPermission(String name, String... actions);

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

    protected class CheckPermission {

        private static final String DEFAULT_ALLOWED_NAME = "someMapsPermission";
        private static final String DEFAULT_REQUESTED_NAME = "someMapsPermission";

        private String requested = null;
        private String[] allowed = null;
        private Boolean expectedResult = null;
        private String allowedName = DEFAULT_ALLOWED_NAME;
        private String requestedName = DEFAULT_REQUESTED_NAME;

        CheckPermission withRequestedName(String requestedName) {
            this.requestedName = requestedName;
            return this;
        }

        CheckPermission withAllowedName(String allowedName) {
            this.allowedName = allowedName;
            return this;
        }

        CheckPermission of(String requested) {
            this.requested = requested;
            return this;
        }

        CheckPermission against(String... allowed) {
            this.allowed = allowed;
            return this;
        }

        CheckPermission expect(boolean expectedResult) {
            this.expectedResult = expectedResult;
            return this;
        }

        void run() {
            if (requested != null && allowed != null && expectedResult != null) {
                Permission allowedPermissions = createPermission(allowedName, allowed);
                Permission requestedPermission = createPermission(requestedName, requested);

                boolean actualResult = allowedPermissions.implies(requestedPermission);

                assertEquals("Access applied incorrectly for requested action of " + requestedPermission
                        + " on permitted permissions of " + allowedPermissions, expectedResult.booleanValue(), actualResult);
            } else {
                fail("requested and/or allowed and/or expect not set");
            }
        }
    }

}

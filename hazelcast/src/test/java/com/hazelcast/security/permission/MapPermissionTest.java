package com.hazelcast.security.permission;

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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Map Permission Tests. A small selection of combinations are used here, use an array permutation algorithm.
 * <p>
 * Could use <a href="http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/Collections2.html#permutations(java.util.Collection)">Google Guava Permutations</a>
 *
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapPermissionTest {

    @Test
    public void willReturnFalseForNoPermOnPut(){
        new CheckPermission().of("put").against("read","create").expect(false).run();
    }

    @Test
    public void willReturnFalseForNoPermOnListen(){
        new CheckPermission().of("listen").against("read","create","put").expect(false).run();
    }

    @Test
    public void willReturnFalseForNoPermOnIndex(){
        new CheckPermission().of("index").against("read","create","put","intercept").expect(false).run();
    }

    private static class CheckPermission {

        private String requested = null;
        private String[] allowed = null;
        private Boolean expectedResult = null;

        CheckPermission of(String requested){
            this.requested = requested;
            return this;
        }

        CheckPermission against(String... allowed){
            this.allowed = allowed;
            return this;
        }

        CheckPermission expect(boolean expectedResult){
            this.expectedResult = expectedResult;
            return this;
        }

        void run(){
            if (requested != null && allowed != null && expectedResult != null){
                MapPermission allowedMapPermissions = new MapPermission("someMapsPermission",allowed);
                MapPermission requestedMapPermission = new MapPermission("someMapsPermission",requested);

                boolean actualResult = allowedMapPermissions.implies(requestedMapPermission);

                assertEquals("Access applied incorrectly for requested action of " + requestedMapPermission + " on permitted permissions of " + allowedMapPermissions,expectedResult.booleanValue(),actualResult);
            } else {
                fail("requested and/or allowed and/or expect not set");
            }
        }

    }

}

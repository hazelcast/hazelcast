/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.Permission;

import static org.junit.Assert.assertNotNull;

/*
 * Defined as different test from `UTFEncoderDecoderTest`
 * because this test plays with system's `SecurityManager` to simulate reflection fails case.
 * So this means that this test can effect others,
 * if runs together as parallel with other tests in `UTFEncoderDecoderTest`.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class UTFEncoderDecoderStringCreatorTest {

    // Issue https://github.com/hazelcast/hazelcast/issues/5777
    @Test
    public void stringCreaterShouldNotBeNullIfFastStringEnabledAndReflectionFails() {
        SecurityManager currentSecurityManager = System.getSecurityManager();
        try {
            UTFEncoderDecoder.StringCreator stringCreator1 = UTFEncoderDecoder.createStringCreator(true);
            assertNotNull(stringCreator1);

            // We assume that current security manager (if exist), allows to set security manager
            System.setSecurityManager(new StringClassAccessBlockerSecurityManager());
            UTFEncoderDecoder.StringCreator stringCreator2 = UTFEncoderDecoder.createStringCreator(true);
            assertNotNull(stringCreator2);
        } finally {
            System.setSecurityManager(currentSecurityManager);
        }
    }

    private static class StringClassAccessBlockerSecurityManager extends SecurityManager {

        @Override
        public void checkPermission(Permission perm) {
            // Do nothing means all permission checks are OK
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // Do nothing means all permission checks are OK
        }

        @Override
        public void checkMemberAccess(Class<?> clazz, int which) {
            if (String.class.equals(clazz)) {
                throw new SecurityException("Accesses to String class are blocked!");
            }
        }

    }

}

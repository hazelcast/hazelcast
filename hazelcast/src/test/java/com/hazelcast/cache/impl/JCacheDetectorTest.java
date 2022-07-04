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

package com.hazelcast.cache.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cache.impl.JCacheDetector.isJCacheAvailable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JCacheDetectorTest extends HazelcastTestSupport {

    private final ILogger logger = Logger.getLogger(JCacheDetectorTest.class);

    @Test
    public void testConstructor() {
        assertUtilityConstructor(JCacheDetector.class);
    }

    @Test
    public void testIsJCacheAvailable_withCorrectVersion() {
        JCacheDetector.ClassAvailabilityChecker classAvailabilityChecker = (className) -> true;

        assertTrue(isJCacheAvailable(classAvailabilityChecker));
    }

    @Test
    public void testIsJCacheAvailable_withCorrectVersion_withLogger() {
        JCacheDetector.ClassAvailabilityChecker classAvailabilityChecker = (className) -> true;

        assertTrue(isJCacheAvailable(logger, classAvailabilityChecker));
    }

    @Test
    public void testIsJCacheAvailable_notFound() {
        JCacheDetector.ClassAvailabilityChecker classAvailabilityChecker = (className) -> false;

        assertFalse(isJCacheAvailable(classAvailabilityChecker));
    }

    @Test
    public void testIsJCacheAvailable_notFound_withLogger() {
        JCacheDetector.ClassAvailabilityChecker classAvailabilityChecker = (className) -> false;

        assertFalse(isJCacheAvailable(logger, classAvailabilityChecker));
    }

    @Test
    public void testIsJCacheAvailable_withWrongJCacheVersion() {
        JCacheDetector.ClassAvailabilityChecker classAvailabilityChecker = (className) -> className.equals("javax.cache.Caching");

        assertFalse(isJCacheAvailable(classAvailabilityChecker));
    }

    @Test
    public void testIsJCacheAvailable_withWrongJCacheVersion_withLogger() {
        JCacheDetector.ClassAvailabilityChecker classAvailabilityChecker = (className) -> className.equals("javax.cache.Caching");

        assertFalse(isJCacheAvailable(logger, classAvailabilityChecker));
    }
}

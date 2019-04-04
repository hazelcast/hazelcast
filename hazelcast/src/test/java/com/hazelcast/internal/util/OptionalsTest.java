/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.RequiresJdk8;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Optional;

import static com.hazelcast.internal.PlatformSpecific.OPTIONALS;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

@Category({QuickTest.class, ParallelTest.class})
public class OptionalsTest {

    @Test
    public void testPreJava8() {
        assumeTrue(JavaVersion.isAtMost(JavaVersion.JAVA_1_7));

        assertInstanceOf(PreJava8Optionals.class, OPTIONALS);

        Object value = new Object();
        assertSame(value, OPTIONALS.unwrapIfOptional(value));

        assertNull(OPTIONALS.unwrapIfOptional(null));
    }

    @RequiresJdk8
    @Test
    public void testJava8Plus() {
        assumeTrue(JavaVersion.isAtLeast(JavaVersion.JAVA_1_8));

        assertInstanceOf(Java8PlusOptionals.class, OPTIONALS);

        Object value = new Object();
        assertSame(value, OPTIONALS.unwrapIfOptional(value));

        assertNull(OPTIONALS.unwrapIfOptional(null));

        assertSame(value, OPTIONALS.unwrapIfOptional(Optional.of(value)));

        assertNull(OPTIONALS.unwrapIfOptional(Optional.empty()));
    }

}

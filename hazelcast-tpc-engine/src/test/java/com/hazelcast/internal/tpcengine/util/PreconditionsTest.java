/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import java.util.LinkedList;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

public class PreconditionsTest {

    @Test
    public void test_checkNotNegative_whenZero() {
        checkNotNegative(0, "foo");
    }

    @Test
    public void test_checkNotNegative_whenPositive() {
        checkNotNegative(1, "foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkNotNegative_whenNegative() {
        checkNotNegative(-1, "foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkPositive_whenZero() {
        checkPositive(0, "foo");
    }

    @Test
    public void test_checkPositive_whenPositive() {
        checkNotNegative(1, "foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkPositive_whenNegative() {
        checkPositive(-1, "foo");
    }

    @Test(expected = NullPointerException.class)
    public void test_checkNotNull2_whenNull() {
        checkNotNull(null, "foo");
    }

    @Test
    public void test_checkNotNull2_whenNotNull() {
        checkNotNull(new LinkedList(), "foo");
    }

    @Test(expected = NullPointerException.class)
    public void test_checkNotNull1_whenNull() {
        checkNotNull(null);
    }

    @Test
    public void test_checkNotNull1_whenNotNull() {
        checkNotNull(new LinkedList());
    }
}

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

package com.hazelcast.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Collection of convenience methods not suitable for production usage due performance characteristics, but handy for tests.
 */
public final class TestCollectionUtils {

    private TestCollectionUtils() {
    }

    /**
     * Creates a new set containing items passed as arguments.
     *
     * @return a new instance of Set with all items passed as an argument
     */
    public static <T> Set<T> setOf(T... items) {
        List<T> list = Arrays.asList(items);
        return new HashSet<T>(list);
    }

    public static Set<Integer> setOfValuesBetween(int from, int to) {
        HashSet<Integer> set = new HashSet<Integer>();
        for (int i = from; i < to; i++) {
            set.add(i);
        }
        return set;
    }
}

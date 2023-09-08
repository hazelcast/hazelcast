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

package com.hazelcast.test;

import java.util.HashSet;
import java.util.Set;

/**
 * Collection of convenience methods not suitable for production usage due performance characteristics, but handy for tests.
 */
public final class TestCollectionUtils {

    private TestCollectionUtils() {
    }

    public static Set<Integer> setOfValuesBetween(int from, int to) {
        HashSet<Integer> set = new HashSet<>();
        for (int i = from; i < to; i++) {
            set.add(i);
        }
        return set;
    }
}

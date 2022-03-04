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

package com.hazelcast.osgi.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.osgi.HazelcastOSGiInstance;
import com.hazelcast.osgi.HazelcastOSGiService;

import static org.mockito.Mockito.mock;

public final class HazelcastOSGiTestUtil {

    private HazelcastOSGiTestUtil() {
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance(HazelcastInstance instance, HazelcastOSGiService service) {
        return new HazelcastOSGiInstanceImpl(instance, service);
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance(HazelcastInstance instance) {
        return createHazelcastOSGiInstance(instance, mock(HazelcastOSGiService.class));
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance(HazelcastOSGiService service) {
        return createHazelcastOSGiInstance(mock(HazelcastInstance.class), service);
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance() {
        return createHazelcastOSGiInstance(mock(HazelcastInstance.class), mock(HazelcastOSGiService.class));
    }
}

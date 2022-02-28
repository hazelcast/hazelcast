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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;

public final class TestConfigUtils {

    public static final int NON_DEFAULT_BACKUP_COUNT = MapConfig.MAX_BACKUP_COUNT;
    public static final InMemoryFormat NON_DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.OBJECT;

    private TestConfigUtils() {
    }
}

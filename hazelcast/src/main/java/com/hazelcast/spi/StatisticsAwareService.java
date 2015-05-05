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

package com.hazelcast.spi;

import com.hazelcast.monitor.LocalInstanceStats;
import com.hazelcast.spi.annotation.Beta;

import java.util.Map;

/**
 *
 * This interface is in BETA stage and is subject to change in upcoming releases.
 *
 */
@Beta
public interface StatisticsAwareService {

    <T extends LocalInstanceStats> Map<String, T> getStats();

}

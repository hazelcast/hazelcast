/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.spi.annotation.Beta;

/**
 * Allows notifying Hazelcast code that the object implementing this interface
 * is effectively immutable.
 * This may mean that it either does not have any state (e.g. pure function)
 * or the state is not mutated at any point.
 * This interface allows for performance optimisations where applicable such
 * as avoiding cloning user supplied objects or cloning hazelcast internal
 * objects supplied to the user.
 * It is important that the user follows the rules:
 * <ul>
 * <li>the object must not have any state which is changed by cloning the object</li>
 * <li>the existing state must not be changed</li>
 * </ul>
 * If an object implements this interface but does not follow these rules,
 * the results of the execution are undefined.
 */
@Beta
public interface Immutable {
}

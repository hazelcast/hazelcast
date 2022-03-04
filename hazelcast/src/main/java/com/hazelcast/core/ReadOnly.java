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

package com.hazelcast.core;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

/**
 * Allows notifying Hazelcast that the processing unit implementing this interface will not do any modifications.
 * This marker interface allows optimizing the processing to gain more performance.
 *
 * If the processing processing unit implementing this interface does a modification an exception will be thrown.
 *
 * Currently supported in:
 * <ul>
 * <li>{@link EntryProcessor} passed to {@link IMap#executeOnKey(Object, EntryProcessor)}</li>
 * <li>{@link EntryProcessor} passed to {@link IMap#submitToKey(Object, EntryProcessor)} </li>
 * </ul>
 *
 * @see Offloadable
 */
public interface ReadOnly {
}

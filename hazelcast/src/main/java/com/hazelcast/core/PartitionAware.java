/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

/**
 * PartitionAware allows implementing keys to be located on the same member
 * or implementing tasks to be executed on {@link #getPartitionKey()}'s owner member.
 * This makes related data to be stored in the same location. (See data-affinity.)
 *
 * @param <T> key type
 */
public interface PartitionAware<T> {

    T getPartitionKey();

}

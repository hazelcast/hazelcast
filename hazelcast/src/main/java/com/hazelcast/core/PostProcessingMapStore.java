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
 * Implement this interface if you modify the value in MapStore.store(K key, V value) method.
 * Otherwise already serialized form will be used to put into hazelcast map and
 * modifications made inside store() method will be discarded. Applying changes done in store()
 * is only possible with write-through configured map store.
 */
public interface PostProcessingMapStore {
}

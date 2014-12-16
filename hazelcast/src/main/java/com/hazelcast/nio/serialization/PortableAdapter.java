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

package com.hazelcast.nio.serialization;

/**
 * This is a base interface for adapting non-Portable classes to Portable.
 * <p/>
 * In situations where it's not possible to modify an existing class
 * to implement Portable and/or it's not allowed to import a Hazelcast
 * class into a domain class, it will be possible to add Portable features
 * to a non-Portable object over this adapter.
 * <p/>
 * Sample:
 *
 * --- SAMPLE GOES HERE ---
 *
 * @see com.hazelcast.nio.serialization.Portable
 *
 * @param <T> type of non-Portable object
 */

// TODO: INTERFACE IS NOT DEFINED YET!
public interface PortableAdapter<T> extends Portable {

}

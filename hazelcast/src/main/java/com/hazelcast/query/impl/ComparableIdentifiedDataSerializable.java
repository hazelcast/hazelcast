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

package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * Defines a contract of objects that are simultaneously {@link Comparable} and
 * {@link IdentifiedDataSerializable}.
 * <p>
 * This contract basically allows to avoid a leakage of the internal
 * implementation details to the outside world if a class is required to be
 * {@link Comparable} by some of its consumers and {@link
 * IdentifiedDataSerializable} by the others.
 * <p>
 * See {@link AbstractIndex#NULL} and {@link CompositeValue#NEGATIVE_INFINITY}
 * for examples.
 */
public interface ComparableIdentifiedDataSerializable extends Comparable, IdentifiedDataSerializable {

}

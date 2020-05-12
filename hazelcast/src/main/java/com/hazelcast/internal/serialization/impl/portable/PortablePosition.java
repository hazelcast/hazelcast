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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.FieldType;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Position of an object in the portable byte stream.
 * <p>
 * Can be used by a reader to read the object at a specified stream position under a specified path.
 * <p>
 * E.g. car.wheels[0].pressure -> the position will point to the pressure object
 */
interface PortablePosition {

    /**
     * @return stream position of the object in the byte stream
     */
    int getStreamPosition();

    /**
     * @return the array index of the object in the byte stream, should the path point to an array at the leaf.
     * e.g. car.wheel[1] - in this case index would be 1. If no quantifier specified the index is -1.
     */
    int getIndex();

    /**
     * @return true if the object under the given path is null, false otherwise.
     */
    boolean isNull();

    /**
     * @return true should the path point to an array and the array is empty, false otherwise.
     */
    boolean isEmpty();

    /**
     * @return length of the array should the path point to an array, -1 otherwise.
     */
    int getLen();

    /**
     * @return portable factoryId should the path point to a portable, -1 otherwise
     */
    int getFactoryId();

    /**
     * @return portable classId should the path point to a portable, -1 otherwise
     */
    int getClassId();

    /**
     * Determines type of position. There's a PortableSinglePosition and PortableMultiPosition (although both private)
     * A PortableMultiPosition is just a grouping object for PortableSinglePosition that implements the PortablePosition
     * interface. It has a common ancestor, thus we can return a single result or multiple results from a method
     * returning a PortablePosition. In this way we avoid extra allocation of a list if there's only a single result.
     *
     * @return true, if the current position is a multi-position, false otherwise.
     */
    boolean isMultiPosition();

    /**
     * Convenience to check if null or empty.
     *
     * @return true if the {@link this.isNull()} or {@link this.isEmpty() } call return true.
     */
    boolean isNullOrEmpty();

    /**
     * It may sometimes happen that navigating to the leaf is impossible since an attribute in between is null.
     * E.g. car.wheels[0].pressure -> wheels array is null, in this case isLeaf() will return false since it didn't
     * manage to reach the leaf of the path.
     *
     * @return true if the portable position points to the leaf of the expression, false otherwise.
     */
    boolean isLeaf();

    /**
     * @return true if the given path contained [any] operator
     */
    boolean isAny();

    /**
     * @return the type of the field under the leaf of the given path. May be null in a couple of scenarios:
     * the call to {@link this.isNull()} == true, positions points to an unknown field or it's a multi-position.
     */
    @Nullable
    FieldType getType();

    /**
     * @return list of PortablePositions if the given object is a MultiPosition, exception is thrown otherwise
     */
    List<PortablePosition> asMultiPosition();

}

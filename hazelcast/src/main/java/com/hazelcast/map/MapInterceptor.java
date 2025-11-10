/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.spi.annotation.NamespacesSupported;

import java.io.Serializable;

/**
 * MapInterceptor is used to intercept changes to the map, allowing access to
 * the values before and after adding them to the map.
 * <p>
 * MapInterceptors are chained when added to the map, which means that when an
 * interceptor is added on node initialization, it could be added twice. To
 * prevent this, make sure to implement the hashCode method to return the same
 * value for every instance of the class.
 * <p>
 * Serialized instances of this interface are used in client-member
 * communication, so changing an implementation's binary format will render it
 * incompatible with its previous versions.
 */
@NamespacesSupported
@BinaryInterface
public interface MapInterceptor extends Serializable {

    /**
     * Intercepts the get operation before returning value.
     * <p>
     * Returns another object to change the return value of get(...) operations.
     * Returning {@code null} will cause the get(...) operation to return the
     * original value, so return {@code null} if you do not want to change
     * anything.
     * <p>
     * When the in-memory format is not {@code OBJECT}, mutations made to the {@code value}
     * do not affect the stored value. They only affect the returned value.
     * <p>
     * When the in-memory format is {@code OBJECT}, try to avoid modifying the input {@code value} directly,
     * as it is not cloned for performance reasons. Mutating it may cause unexpected behavior:
     * <ul>
     *   <li>The modified {@code value} will be persisted in the map.</li>
     *   <li>The modification will take effect even if the method returns {@code null}.</li>
     *   <li>In a chain of interceptors, if one interceptor modifies the input value,
     *   the next interceptor will receive the modified value as input.</li>
     * </ul>
     * If changes are required, create and return a copy of the value instead.
     * <p>
     * Modifying input values is not supported when in-memory-format=OBJECT is used in combination with indexes.
     * Doing so will lead to runtime errors.
     *
     * @param value the original value to be returned as the result of get(...)
     *              operation
     * @return the new value that will be returned by the get(...) operation
     */
    Object interceptGet(Object value);

    /**
     * Called after the get(...) operation is completed.
     * <p>
     * For all in-memory formats mutations made to {@code value} do not affect the stored value.
     *
     * @param value the value returned as the result of the get(...) operation
     */
    void afterGet(Object value);

    /**
     * Intercepts the put operation before modifying the map data.
     * <p>
     * Returns the object to be put into the map. Returning {@code null} will
     * cause the put(...) operation to operate as expected, namely no
     * interception. Throwing an exception will cancel the put operation.
     * <p>
     * When the in-memory format is {@code OBJECT}, try to avoid modifying the input values directly,
     * as it is not cloned for performance reasons. Mutating it may cause unexpected behavior:
     * <ul>
     *   <li>The modification of {@code newValue} will take effect even if the method returns {@code null}.</li>
     *   <li>In a chain of interceptors, if one interceptor modifies the input value,
     *   the next interceptor will receive the modified value as input.</li>
     * </ul>
     * If modification is necessary, create and return a copy instead.
     *
     * @param oldValue the value currently in map
     * @param newValue the new value to be put into the map
     * @return new value after the intercept operation
     */
    Object interceptPut(Object oldValue, Object newValue);

    /**
     * Called after the put(...) operation is completed.
     * <p>
     * When the in-memory format is not {@code OBJECT}, mutations made to the {@code value}
     * do not affect the stored value.
     * <p
     * When the in-memory format is {@code OBJECT}, try to avoid modifying the input {@code value} directly,
     * as it is not cloned for performance reasons. Mutating it may cause unexpected behavior:
     * <ul>
     *   <li>The modified {@code value} will be persisted in the map.</li>
     *   <li>In a chain of interceptors, if one interceptor modifies the input value,
     *   the next interceptor will receive the modified value as input.</li>
     * </ul>
     *
     * @param value the new value that was put into the map.
     */
    void afterPut(Object value);

    /**
     * Intercepts the remove operation before removing the data.
     * <p>
     * Returns the object to be returned as the result of the remove operation.
     * Throwing an exception will cancel the remove operation.
     * <p>
     * When the in-memory format is {@code OBJECT}, try to avoid modifying the input {@code value} directly,
     * as it is not cloned for performance reasons. Mutating it may cause unexpected behavior:
     * <ul>
     *   <li>The modification of {@code removedValue} will take effect even if the method returns {@code null}.</li>
     *   <li>In a chain of interceptors, if one interceptor modifies the input value,
     *   the next interceptor will receive the modified value as input.</li>
     * </ul>
     * If modification is necessary, create and return a copy instead.
     * <p>
     * Modifying input values is not supported when in-memory format {code OBJECT} is used in combination with indexes.
     * Doing so will lead to runtime errors.
     *
     * @param removedValue the existing value to be removed
     * @return the value to be returned as the result of remove operation
     */
    Object interceptRemove(Object removedValue);

    /**
     * Called after the remove(...) operation is completed.
     *
     * @param oldValue the value returned as the result of the remove(...)
     *                 operation
     */
    void afterRemove(Object oldValue);
}

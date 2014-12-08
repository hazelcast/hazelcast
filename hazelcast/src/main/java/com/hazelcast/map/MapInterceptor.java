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

package com.hazelcast.map;

import java.io.Serializable;

/**
 * MapInterceptor is used to intercept changes to the map, allowing access to the values before and
 * after adding it to the map
 *
 * MapInterceptors are chained when added to the map, which means that when an interceptor is added
 * on node initialization it could be added twice. To prevent this make sure to implement the
 * hashCode method to return the same value for every instance of the class.
 *
 */
public interface MapInterceptor extends Serializable {

    /**
     * Intercept get operation before returning value.
     * Return another object to change the return value of get(..)
     * Returning null will cause the get(..) operation return original value,
     * namely return null if you do not want to change anything.
     * <p/>
     * Mutations made to value do not affect the stored value. They do affect the returned value.
     *
     * @param value the original value to be returned as the result of get(..) operation
     * @return the new value that will be returned by get(..) operation
     */
    Object interceptGet(Object value);

    /**
     * Called after get(..) operation is completed.
     * <p/>
     * Mutations made to value do not affect the stored value.
     *
     * @param value the value returned as the result of get(..) operation
     */
    void afterGet(Object value);

    /**
     * Intercept put operation before modifying map data.
     * Return the object to be put into the map.
     * Returning null will cause the put(..) operation to operate as expected, namely no interception.
     * Throwing an exception will cancel the put operation.
     * <p/>
     *
     * @param oldValue the value currently in map
     * @param newValue the new value to be put
     * @return new value after intercept operation
     */
    Object interceptPut(Object oldValue, Object newValue);

    /**
     * Called after put(..) operation is completed.
     * <p/>
     *
     * @param value the value returned as the result of put(..) operation
     */
    void afterPut(Object value);

    /**
     * Intercept remove operation before removing the data.
     * Return the object to be returned as the result of remove operation.
     * Throwing an exception will cancel the remove operation.
     * <p/>
     *
     * @param removedValue the existing value to be removed
     * @return the value to be returned as the result of remove operation
     */
    Object interceptRemove(Object removedValue);

    /**
     * Called after remove(..) operation is completed.
     * <p/>
     *
     * @param value the value returned as the result of remove(..) operation
     */
    void afterRemove(Object value);

}

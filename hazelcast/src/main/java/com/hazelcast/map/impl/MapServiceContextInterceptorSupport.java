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

package com.hazelcast.map.impl;

import com.hazelcast.map.MapInterceptor;

/**
 * Helper interceptor methods for {@link MapServiceContext}.
 */
public interface MapServiceContextInterceptorSupport {

    /**
     * Intercept the get operation.
     * <p>
     * This method can modify the input {@Code value}, so use with caution.
     *
     * @param interceptorRegistry the interceptor registry
     * @param value               the value to be intercepted
     * @return the intercepted value
     */
    Object interceptGet(InterceptorRegistry interceptorRegistry, Object value);

    /**
     * Intercept the get operation after the value is retrieved.
     * <p>
     * This method can modify the input {@Code value}, so use with caution.
     *
     * @param interceptorRegistry the interceptor registry
     * @param value               the value to be intercepted
     */
    void interceptAfterGet(InterceptorRegistry interceptorRegistry, Object value);

    /**
     * Intercepts the put operation before the new value is stored.
     * <p>
     * This method can modify the input {@Code newValue} and {@Code oldValue}, so use with caution.
     *
     * @param interceptorRegistry the interceptor registry
     * @param oldValue            the old value to be intercepted
     * @param newValue            the new value to be intercepted
     * @return the intercepted value
     */
    Object interceptPut(InterceptorRegistry interceptorRegistry, Object oldValue, Object newValue);

    /**
     * Intercept the put operation after the new value is stored.
     * <p>
     * This method can modify the input {@Code newValue}, so use with caution.
     *
     * @param interceptorRegistry the interceptor registry
     * @param newValue            the new value to be intercepted
     */
    void interceptAfterPut(InterceptorRegistry interceptorRegistry, Object newValue);

    /**
     * Intercept the remove operation before the value is removed.
     * <p>
     * This method can modify the input {@Code value}, so use with caution.
     *
     * @param interceptorRegistry the interceptor registry
     * @param value               the value to be intercepted
     * @return the intercepted value
     */
    Object interceptRemove(InterceptorRegistry interceptorRegistry, Object value);

    /**
     * Intercept the remove operation after the value is removed.
     * <p>
     * This method can modify the input {@Code value}, so use with caution.
     *
     * @param interceptorRegistry the interceptor registry
     * @param value               the value to be intercepted
     */
    void interceptAfterRemove(InterceptorRegistry interceptorRegistry, Object value);

    String generateInterceptorId(String mapName, MapInterceptor interceptor);

    void addInterceptor(String id, String mapName, MapInterceptor interceptor);

    boolean removeInterceptor(String mapName, String id);
}

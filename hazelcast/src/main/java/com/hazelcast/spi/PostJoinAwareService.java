/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

/**
 * @author mdogan 1/11/13
 */
public interface PostJoinAwareService {

    /**
     * Post join operations must be lock free, meaning no locks at all:
     * no partition locks, no key-based locks, no service level locks,
     * no database interaction!
     * <p>
     * Post join operations should return response, at least a null response.
     * @return a response from the post join operation. Can be null.
     */
    Operation getPostJoinOperation();

}

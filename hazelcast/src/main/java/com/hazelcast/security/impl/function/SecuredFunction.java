/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security.impl.function;

import com.hazelcast.spi.annotation.PrivateApi;

import java.security.Permission;

/**
 * A function which requires a permission to run, see {@link #permission()}.
 * The default implementation returns {@code null} which means no
 * permission is required to run this function.
 *
 * @see SecuredFunctions
 */
@PrivateApi
public interface SecuredFunction {

    /**
     * @return the required permission to run this function
     */
    default Permission permission() {
        return null;
    }

}

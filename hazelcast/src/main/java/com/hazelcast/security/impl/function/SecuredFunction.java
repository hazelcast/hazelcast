/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nullable;
import java.security.Permission;
import java.util.List;

/**
 * A function which requires some permissions to run, see {@link #permissions()}.
 * The default implementation returns {@code null} which means no
 * permission is required to run this function.
 *
 * @see SecuredFunctions
 */
@PrivateApi
public interface SecuredFunction {

    /**
     * @return the list of permissions required to run this function
     *
     * @implNote If used, permissions should be hardcoded based on function arguments/context.
     * It is not secure to keep {@code List<Permission>} in the function itself because
     * it could be tampered with if the function is serialized and passed from untrusted caller.
     */
    @Nullable
    default List<Permission> permissions() {
        return null;
    }
}

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

package com.hazelcast.security.permission;

public class SqlPermission extends InstancePermission {

    private static final int CREATE_MAPPING = CREATE;
    private static final int DROP_MAPPING = DESTROY;
    private static final int CREATE_INDEX = DROP_MAPPING << 1;
    private static final int ALL = CREATE_MAPPING | DROP_MAPPING | CREATE_INDEX;

    public SqlPermission(String name, String... actions) {
        super(name, actions);
    }

    @Override
    protected int initMask(String[] actions) {
        int mask = NONE;
        for (String action : actions) {
            if (ActionConstants.ACTION_ALL.equals(action)) {
                return ALL;
            } else {
                if (ActionConstants.ACTION_CREATE.equals(action)) {
                    mask |= CREATE_MAPPING;
                } else if (ActionConstants.ACTION_DESTROY.equals(action)) {
                    mask |= DROP_MAPPING;
                } else if (ActionConstants.ACTION_INDEX.equals(action)) {
                    mask |= CREATE_INDEX;
                }
                // Note: DROP INDEX is not implemented yet, no need to have separate permission.
            }
        }
        return mask;
    }
}

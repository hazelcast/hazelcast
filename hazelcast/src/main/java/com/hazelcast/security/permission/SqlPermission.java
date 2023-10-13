/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
    private static final int CREATE_VIEW = CREATE_INDEX << 1;
    private static final int DROP_VIEW = CREATE_VIEW << 1;
    private static final int CREATE_TYPE = DROP_VIEW << 1;
    private static final int DROP_TYPE = CREATE_TYPE << 1;
    private static final int VIEW_DATACONNECTION = DROP_TYPE << 1;
    private static final int CREATE_DATACONNECTION = VIEW_DATACONNECTION << 1;
    private static final int DROP_DATACONNECTION = CREATE_DATACONNECTION << 1;
    private static final int VIEW_MAPPING = DROP_DATACONNECTION << 1;
    private static final int ALL = CREATE_MAPPING | DROP_MAPPING
            | CREATE_INDEX
            | CREATE_VIEW | DROP_VIEW
            | CREATE_TYPE | DROP_TYPE
            | VIEW_DATACONNECTION | CREATE_DATACONNECTION | DROP_DATACONNECTION
            // Note: added to the tail for backward compatibility.
            | VIEW_MAPPING;

    public SqlPermission(String name, String... actions) {
        super(name, actions);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    protected int initMask(String[] actions) {
        int mask = NONE;
        for (String action : actions) {
            if (ActionConstants.ACTION_ALL.equals(action)) {
                return ALL;
            } else {
                // Note: mappings permission are mapped to the standard ACTION_* permissions
                if (ActionConstants.ACTION_CREATE.equals(action)) {
                    mask |= CREATE_MAPPING;
                } else if (ActionConstants.ACTION_DESTROY.equals(action)) {
                    mask |= DROP_MAPPING;
                } else if (ActionConstants.ACTION_INDEX.equals(action)) {
                    mask |= CREATE_INDEX;
                } else if (ActionConstants.ACTION_CREATE_VIEW.equals(action)) {
                    mask |= CREATE_VIEW;
                } else if (ActionConstants.ACTION_DROP_VIEW.equals(action)) {
                    mask |= DROP_VIEW;
                } else if (ActionConstants.ACTION_CREATE_TYPE.equals(action)) {
                    mask |= CREATE_TYPE;
                } else if (ActionConstants.ACTION_DROP_TYPE.equals(action)) {
                    mask |= DROP_TYPE;
                } else if (ActionConstants.ACTION_VIEW_DATACONNECTION.equals(action)) {
                    mask |= VIEW_DATACONNECTION;
                } else if (ActionConstants.ACTION_CREATE_DATACONNECTION.equals(action)) {
                    mask |= CREATE_DATACONNECTION;
                } else if (ActionConstants.ACTION_DROP_DATACONNECTION.equals(action)) {
                    mask |= DROP_DATACONNECTION;
                } else if (ActionConstants.ACTION_VIEW_MAPPING.equals(action)) {
                    mask |= VIEW_MAPPING;
                }
                // Note: DROP INDEX is not implemented yet, no need to have separate permission.
            }
        }
        return mask;
    }
}

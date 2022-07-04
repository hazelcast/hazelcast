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

package com.hazelcast.security.permission;

public class JobPermission extends InstancePermission {

    private static final int SUBMIT = 1;
    private static final int CANCEL = 2;
    private static final int READ = 4;
    private static final int RESTART = 8;
    private static final int EXPORT_SNAPSHOT = 16;
    private static final int ADD_RESOURCES = 32;
    private static final int ALL = SUBMIT | CANCEL | READ | RESTART | EXPORT_SNAPSHOT | ADD_RESOURCES;


    public JobPermission(String... actions) {
        super("<job>", actions);
    }

    @Override
    protected int initMask(String[] actions) {
        int mask = READ;
        for (String action : actions) {
            if (ActionConstants.ACTION_ALL.equals(action)) {
                return ALL;
            }

            if (ActionConstants.ACTION_SUBMIT.equals(action)) {
                mask |= SUBMIT;
            } else if (ActionConstants.ACTION_CANCEL.equals(action)) {
                mask |= CANCEL;
            } else if (ActionConstants.ACTION_READ.equals(action)) {
                mask |= READ;
            } else if (ActionConstants.ACTION_RESTART.equals(action)) {
                mask |= RESTART;
            } else if (ActionConstants.ACTION_EXPORT_SNAPSHOT.equals(action)) {
                mask |= EXPORT_SNAPSHOT;
            } else if (ActionConstants.ACTION_ADD_RESOURCES.equals(action)) {
                mask |= ADD_RESOURCES;
            } else {
                throw new IllegalArgumentException("Configured action[" + action + "] is not supported");
            }
        }
        return mask;
    }
}

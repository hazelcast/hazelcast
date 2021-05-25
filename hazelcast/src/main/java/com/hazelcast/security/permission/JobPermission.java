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

public class JobPermission extends InstancePermission {

    private static final int READ = 4;
    private static final int LIST = 8;
    private static final int SUSPEND = 16;
    private static final int RESUME = 32;
    private static final int EXPORT = 64;
    private static final int UPLOAD = 128;
    private static final int ALL = CREATE | DESTROY | READ | LIST | SUSPEND | RESUME | EXPORT | UPLOAD;


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

            if (ActionConstants.ACTION_CREATE.equals(action)) {
                mask |= CREATE;
            } else if (ActionConstants.ACTION_DESTROY.equals(action)) {
                mask |= DESTROY;
            } else if (ActionConstants.ACTION_LIST.equals(action)) {
                mask |= LIST;
            } else if (ActionConstants.ACTION_SUSPEND.equals(action)) {
                mask |= SUSPEND;
            } else if (ActionConstants.ACTION_RESUME.equals(action)) {
                mask |= RESUME;
            } else if (ActionConstants.ACTION_EXPORT.equals(action)) {
                mask |= EXPORT;
            } else if (ActionConstants.ACTION_UPLOAD.equals(action)) {
                mask |= UPLOAD;
            } else {
                throw new IllegalArgumentException("Configured action[" + action + "] is not supported");
            }
        }
        return mask;
    }
}

/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security.jsm;

import java.security.BasicPermission;

/**
 * Class which holds named Hazelcast permissions for Security Manager checks. The list of actions is unused in this class.
 * Permission name supports {@code *} wildcard as defined in {@link BasicPermission} class.
 * <p>
 * Usage of protected method name as a target name is recommended. E.g.
 *
 * <pre>
 * SecurityManager sm = System.getSecurityManager();
 * if (sm != null) {
 *     sm.checkPermission(new HazelcastRuntimePermission("com.hazelcast.config.Config.getLicenseKey"));
 * }
 * </pre>
 */
public class HazelcastRuntimePermission extends BasicPermission {

    private static final long serialVersionUID = -8927678876656102420L;

    /**
     * Creates permission with given name.
     */
    public HazelcastRuntimePermission(String name) {
        super(name);
    }

    /**
     * Creates permission with given name. The actions list is ignored.
     */
    public HazelcastRuntimePermission(String name, String actions) {
        super(name, actions);
    }
}

/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

public class GroupConfig {

    public static final String DEFAULT_GROUP_PASSWORD = "dev-pass";
    public static final String DEFAULT_GROUP_NAME = "dev";

    private String name = DEFAULT_GROUP_NAME;
    private String password = DEFAULT_GROUP_PASSWORD;

    public GroupConfig() {
    }

    public GroupConfig(final String name) {
        setName(name);
    }

    public GroupConfig(final String name, final String password) {
        setName(name);
        setPassword(password);
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public GroupConfig setName(final String name) {
        if (name == null) {
            throw new NullPointerException("group name cannot be null");
        }
        this.name = name;
        return this;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public GroupConfig setPassword(final String password) {
        if (password == null) {
            throw new NullPointerException("group password cannot be null");
        }
        this.password = password;
        return this;
    }
}

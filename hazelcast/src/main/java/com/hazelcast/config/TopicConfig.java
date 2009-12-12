/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

public class TopicConfig {

    public final static boolean DEFAULT_GLOBAL_ORDERING_ENABLED = false;

    private String name;

    private boolean globalOrderingEnabled = DEFAULT_GLOBAL_ORDERING_ENABLED;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the globalOrderingEnabled
     */
    public boolean isGlobalOrderingEnabled() {
        return globalOrderingEnabled;
    }

    /**
     * @param globalOrderingEnabled the globalOrderingEnabled to set
     */
    public void setGlobalOrderingEnabled(boolean globalOrderingEnabled) {
        this.globalOrderingEnabled = globalOrderingEnabled;
    }
}

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

package com.hazelcast.security;

import javax.security.auth.callback.Callback;

import com.hazelcast.config.security.RealmConfig;

/**
 * This JAAS {@link Callback} is used to retrieve a {@link RealmConfig} from client or member configuration.
 */
public class RealmConfigCallback implements Callback {

    private RealmConfig realmConfig;
    private final String realmName;

    public RealmConfigCallback(String realmName) {
        this.realmName = realmName;
    }

    public String getRealmName() {
        return realmName;
    }

    public RealmConfig getRealmConfig() {
        return realmConfig;
    }

    public void setRealmConfig(RealmConfig realmConfig) {
        this.realmConfig = realmConfig;
    }
}

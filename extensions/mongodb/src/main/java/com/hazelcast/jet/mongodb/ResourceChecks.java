/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.mongodb;

import java.io.Serializable;

/**
 * Defines when and if database and collection existence will be checked.
 * @since 5.4
 */
public enum ResourceChecks implements Serializable {

    /**
     * Check will be done on every connect action on every processor.
     */
    ON_EACH_CONNECT,
    /**
     * Check will be done once per each job lifetime
     */
    ONCE_PER_JOB,
    /**
     * (SQL-only) check will be done only during initial mapping creation.
     */
    ONLY_INITIAL,

    /**
     * Check won't be ever done.
     */
    NEVER;

    /**
     * Resolves given string value to one of values of this enum.
     */
    public static ResourceChecks fromString(String code) {
        if (ONCE_PER_JOB.name().equalsIgnoreCase(code) || "on-each-call".equalsIgnoreCase(code)) {
            return ONCE_PER_JOB;
        }
        if (ON_EACH_CONNECT.name().equalsIgnoreCase(code) || "on-each-connect".equalsIgnoreCase(code)) {
            return ONCE_PER_JOB;
        }
        if (ONLY_INITIAL.name().equalsIgnoreCase(code) || "only-initial".equalsIgnoreCase(code)) {
            return ONLY_INITIAL;
        }
        if (NEVER.name().equalsIgnoreCase(code)) {
            return NEVER;
        }
        throw new IllegalArgumentException("Unknown value for ResourceExistenceChecks:" + code);
    }

    /**
     * @return Is check ever done?
     */
    public boolean isEverPerformed() {
        return this != NEVER;
    }
}

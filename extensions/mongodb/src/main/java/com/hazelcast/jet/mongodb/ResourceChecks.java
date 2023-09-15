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
import java.util.Locale;

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
     * Check will be done once per each job execution (when job starts from scratch or from snapshot).
     */
    ONCE_PER_JOB,

    /**
     * (SQL-only) check will be done only during initial mapping creation.
     */
    ONLY_INITIAL,

    /**
     * Check won't be ever done, meaning that the database and collection will be created automatically by Mongo
     * when it's accessed for the first time.
     */
    NEVER;

    /**
     * Resolves given string value to one of values of this enum.
     */
    public static ResourceChecks fromString(String code) {
        String codeLowerCased = code.toLowerCase(Locale.ROOT);

        switch (codeLowerCased) {
            case "on-each-call": return ONCE_PER_JOB;
            case "on-each-connect": return ON_EACH_CONNECT;
            case "only-initial": return ONLY_INITIAL;
            case "never": return NEVER;
            default: throw new IllegalArgumentException("Unknown value for ResourceExistenceChecks:" + codeLowerCased);
        }
    }

    /**
     * @return Is check ever done?
     */
    public boolean isEverPerformed() {
        return this != NEVER;
    }
}

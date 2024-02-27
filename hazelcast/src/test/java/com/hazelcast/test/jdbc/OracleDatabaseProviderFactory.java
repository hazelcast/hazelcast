/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.jdbc;

import static com.hazelcast.jet.TestedVersions.ORACLE_PROPERTY_NAME;
import static com.hazelcast.jet.TestedVersions.TEST_ORACLE_VERSION;

public class OracleDatabaseProviderFactory {


    private OracleDatabaseProviderFactory() {
    }

    public static TestDatabaseProvider createTestDatabaseProvider() {
        return createTestDatabaseProvider(TEST_ORACLE_VERSION);
    }

    static TestDatabaseProvider createTestDatabaseProvider(String dockerImageName) {
        if (dockerImageName.startsWith("gvenzl/oracle-xe")) {
            return new OracleXeDatabaseProvider(TEST_ORACLE_VERSION);
        } else if (dockerImageName.startsWith("gvenzl/oracle-free")) {
            return new OracleFreeDatabaseProvider(TEST_ORACLE_VERSION);
        } else {
            String msg = "The image name must be 'gvenzl/oracle-xe' or 'gvenzl/oracle-free'. Please provide a valid value for the system property: " + ORACLE_PROPERTY_NAME;
            throw new IllegalArgumentException(msg);
        }
    }
}

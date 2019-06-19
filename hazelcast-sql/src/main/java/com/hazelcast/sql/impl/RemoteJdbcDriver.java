/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.instance.BuildInfoProvider;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.remote.Driver;

public class RemoteJdbcDriver extends Driver {

    static {
        new RemoteJdbcDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:remote:hazelcast:";
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion("Hazelcast Remote JDBC Driver", "0.1", "Hazelcast",
                BuildInfoProvider.getBuildInfo().getVersion(), false, 0, 1, 0, 1);
    }

}

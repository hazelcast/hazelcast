/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataconnection.impl.hazelcastdataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.StringUtil;

import static com.hazelcast.dataconnection.HazelcastDataConnection.CLIENT_XML;
import static com.hazelcast.dataconnection.HazelcastDataConnection.CLIENT_XML_PATH;
import static com.hazelcast.dataconnection.HazelcastDataConnection.CLIENT_YML;
import static com.hazelcast.dataconnection.HazelcastDataConnection.CLIENT_YML_PATH;


public class HazelcastDataConnectionConfigValidator {

    public void validate(DataConnectionConfig dataConnectionConfig) {
        int numberOfSetItems = getNumberOfSetItems(dataConnectionConfig, CLIENT_XML_PATH, CLIENT_YML_PATH, CLIENT_XML,
                CLIENT_YML);
        if (numberOfSetItems != 1) {
            throw new HazelcastException("HazelcastDataConnection with name '" + dataConnectionConfig.getName()
                                         + "' could not be created, "
                                         + "provide either a file path with one of "
                                         + "\"client_xml_path\" or \"client_yml_path\" properties "
                                         + "or a string content with one of \"client_xml\" or \"client_yml\" properties "
                                         + "for the client configuration.");
        }
    }

    private int getNumberOfSetItems(DataConnectionConfig dataConnectionConfig, String... properties) {
        int count = 0;
        for (String property : properties) {
            if (isSetAfterTrim(dataConnectionConfig.getProperty(property))) {
                count++;
            }
        }
        return count;
    }

    private boolean isSetAfterTrim(String s) {
        return !StringUtil.isNullOrEmptyAfterTrim(s);
    }
}

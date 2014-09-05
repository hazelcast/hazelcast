/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getString;
import static java.lang.String.format;

/**
 * Identifier for the ManagementCenter. This information is used when a member identifies itself to the
 * ManagementCenter. It contains information like version/clustername/address.
 */
public class ManagementCenterIdentifier implements JsonSerializable {

    private static final int VERSION_MULTIPLIER = 10;
    private int version;
    private String clusterName;
    private String address;
    private transient String versionString;

    public ManagementCenterIdentifier() {
    }

    public ManagementCenterIdentifier(String version, String clusterName, String address) {
        this.version = getVersionAsInt(version);
        this.clusterName = clusterName;
        this.address = address;
    }

    public static int getVersionAsInt(String versionString) throws IllegalArgumentException {
        int version = 0;
        Pattern pattern = Pattern.compile("^(\\d)\\.(\\d)(?:\\.(\\d))?+.*");
        final Matcher matcher = pattern.matcher(versionString);
        if (matcher.matches()) {
            for (int i = 1; i < matcher.groupCount() + 1; i++) {
                version *= VERSION_MULTIPLIER;
                version += Integer.parseInt(matcher.group(i) == null ? "0" : matcher.group(i));
            }
            return version;
        }
        throw new IllegalArgumentException(format("version string '%s' is not valid", versionString));
    }

    public static String convertVersionToString(int version) {
        StringBuilder builder = new StringBuilder();
        String v = Integer.toString(version);
        builder.append(v.charAt(0));
        builder.append('.');
        builder.append(v.charAt(1));
        builder.append('.');
        builder.append(v.charAt(2));
        return builder.toString();
    }


    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("version", version);
        root.add("clusterName", clusterName);
        root.add("address", address);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        version = getInt(json, "version");
        clusterName = getString(json, "clusterName");
        address = getString(json, "address");
    }

    public int getVersion() {
        return version;
    }

    public String getVersionString() {
        if (versionString == null) {
            versionString = convertVersionToString(version);
        }
        return versionString;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getAddress() {
        return address;
    }
}

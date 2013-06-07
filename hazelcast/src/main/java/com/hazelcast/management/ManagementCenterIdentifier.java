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

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: sancar
 * Date: 6/6/13
 * Time: 1:40 PM
 */
public class ManagementCenterIdentifier implements Serializable {

    private int version;
    private String clusterName;
    private String address;
    public transient String versionString;

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
                version *= 10;
                version += Integer.parseInt(matcher.group(i) == null ? "0" : matcher.group(i));
            }
            return version;
        }
        throw new IllegalArgumentException("version string is not valid");
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

    public void read(InputStream in) throws IOException {
        DataInputStream dataInput = new DataInputStream(in);
        version = dataInput.readInt();
        clusterName = dataInput.readUTF();
        address = dataInput.readUTF();
    }

    public void write(OutputStream out) throws IOException {
        DataOutputStream dataOutput = new DataOutputStream(out);
        dataOutput.writeInt(version);
        dataOutput.writeUTF(clusterName);
        dataOutput.writeUTF(address);
    }

    public int getVersion() {
        return version;
    }

    public String getVersionString() {
        if (versionString == null)
            versionString = convertVersionToString(version);
        return versionString;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getAddress() {
        return address;
    }
}

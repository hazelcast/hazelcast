/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jclouds;


import com.hazelcast.config.InvalidConfigurationException;
import org.jclouds.aws.domain.SessionCredentials;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Build SessionCredentials for JClouds Compute Service API
 * with IAM Role.
 */

public class IAMRoleCredentialSupplierBuilder {

    private static final String IAM_ROLE_ENDPOINT = "169.254.169.254";
    private String roleName;
    private SessionCredentials credentials;
    private String query = "latest/meta-data/iam/security-credentials/";

    public IAMRoleCredentialSupplierBuilder() {
    }

    public IAMRoleCredentialSupplierBuilder withRoleName(String roleName) {
        isNotNull(roleName, "iam-role");
        this.roleName = roleName;
        query = query + roleName;
        return this;
    }

    public String getRoleName() {
        return roleName;
    }

    public SessionCredentials getCredentials() {
        return credentials;
    }

    protected Map<String, String> getKeysFromIamRole() {
        try {
            URL url = new URL("http", IAM_ROLE_ENDPOINT, query);
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
            return parseIamRole(reader);
        } catch (IOException io) {
            throw new InvalidConfigurationException("Invalid Aws Configuration");
        }
    }

    protected Map<String, String> parseIamRole(BufferedReader reader) throws IOException {
        Map map = new HashMap();
        Pattern keyPattern = Pattern.compile("\"(.*?)\" : ");
        Pattern valuePattern = Pattern.compile(" : \"(.*?)\",");
        String line;
        for (line = reader.readLine(); line != null; line = reader.readLine()) {
            if (line.contains(":")) {
                Matcher keyMatcher = keyPattern.matcher(line);
                Matcher valueMatcher = valuePattern.matcher(line);
                if (keyMatcher.find() && valueMatcher.find()) {
                    String key = keyMatcher.group(1);
                    String value = valueMatcher.group(1);
                    map.put(key, value);
                }
            }
        }
        return map;
    }

    public SessionCredentials build() {
        Map<String, String> keyMaps = getKeysFromIamRole();
        credentials = new SessionCredentials.Builder()
                .accessKeyId(keyMaps.get("AccessKeyId"))
                .secretAccessKey(keyMaps.get("SecretAccessKey"))
                .sessionToken(keyMaps.get("Token")).build();
        return credentials;
    }
}

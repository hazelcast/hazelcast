/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws.impl;

import com.hazelcast.aws.security.EC2RequestSigner;
import com.hazelcast.aws.utility.CloudyUtility;
import com.hazelcast.aws.utility.Environment;
import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.InvalidConfigurationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.aws.impl.Constants.DOC_VERSION;
import static com.hazelcast.aws.impl.Constants.SIGNATURE_METHOD_V4;
import static com.hazelcast.nio.IOUtil.closeResource;

public class DescribeInstances {

    public static final String IAM_ROLE_ENDPOINT = "169.254.169.254";
    public static final String IAM_TASK_ROLE_ENDPOINT = "169.254.170.2";

    private EC2RequestSigner rs;
    private AwsConfig awsConfig;
    private String endpoint;
    private Map<String, String> attributes = new HashMap<String, String>();

    public DescribeInstances(AwsConfig awsConfig, String endpoint) throws IOException {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        this.awsConfig = awsConfig;
        this.endpoint = endpoint;

        checkKeysFromIamRoles(new Environment());

        String timeStamp = getFormattedTimestamp();
        rs = new EC2RequestSigner(awsConfig, timeStamp, endpoint);
        attributes.put("Action", this.getClass().getSimpleName());
        attributes.put("Version", DOC_VERSION);
        attributes.put("X-Amz-Algorithm", SIGNATURE_METHOD_V4);
        attributes.put("X-Amz-Credential", rs.createFormattedCredential());
        attributes.put("X-Amz-Date", timeStamp);
        attributes.put("X-Amz-SignedHeaders", "host");
        attributes.put("X-Amz-Expires", "30");
    }

    DescribeInstances(AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        this.awsConfig = awsConfig;
    }

    void checkKeysFromIamRoles(Environment env) throws IOException {
        if (awsConfig.getAccessKey() != null && awsConfig.getIamRole() != null) {
            throw new InvalidConfigurationException("You should only define one of `<iam-role>` and `<access-key>`");
        }
        if (awsConfig.getAccessKey() != null) {
            return;
        }

        // in case no IAM role has been defined, this will attempt to retrieve name of default role.
        tryGetDefaultIamRole();

        // if IAM role is still empty, one last attempt
        if (awsConfig.getIamRole() == null || "".equals(awsConfig.getIamRole())) {
            getKeysFromIamTaskRole(env);

        } else {
            getKeysFromIamRole();
        }
    }

    private void getKeysFromIamTaskRole(Environment env) throws IOException {
        // before giving up, attempt to discover whether we're running in an ECS Container,
        // in which case, AWS_CONTAINER_CREDENTIALS_RELATIVE_URI will exist as an env var.
        String uri = env.getEnvVar(Constants.ECS_CREDENTIALS_ENV_VAR_NAME);
        if (uri == null) {
            throw new IllegalArgumentException("Could not acquire credentials! "
              + "Did not find declared AWS access key or IAM Role, and could not discover IAM Task Role or default role.");
        }
        uri = "http://" + IAM_TASK_ROLE_ENDPOINT + uri;

        String json = "";
        try {
            json = retrieveRoleFromURI(uri);
            parseAndStoreRoleCreds(json);

        } catch (Exception io) {
            throw new InvalidConfigurationException("Unable to retrieve credentials from IAM Task Role. "
              + "URI: " + uri + ". \n HTTP Response content: " + json, io);
        }

    }

    /**
     * This is a helper method that simply performs the HTTP request to retrieve the role, from a given URI.
     * (It allows us to cleanly separate the network calls out of our main code logic, so we can mock in our UT.)
     * @param uri the full URI where a `GET` request will retrieve the role information, represented as JSON.
     * @return The content of the HTTP response, as a String. NOTE: This is NEVER null.
     */
    String retrieveRoleFromURI(String uri) throws IOException {
        StringBuilder response = new StringBuilder();

        InputStreamReader is = null;
        BufferedReader reader = null;
        try {
            URL url = new URL(uri);
            is = new InputStreamReader(url.openStream(), "UTF-8");
            reader = new BufferedReader(is);
            String resp;
            while ((resp = reader.readLine()) != null) {
                response = response.append(resp);
            }
            return response.toString();
        } catch (IOException io) {
            throw new InvalidConfigurationException("Unable to lookup role in URI: " + uri, io);
        } finally {
            if (is != null) {
                is.close();
            }
            if (reader != null) {
                reader.close();
            }
        }

    }

    private void tryGetDefaultIamRole() throws IOException {
        // if none of the below are true
        if (!(
            awsConfig.getIamRole() == null
            || "".equals(awsConfig.getIamRole())
            || "DEFAULT".equals(awsConfig.getIamRole())
            )
        ) {
          // stop here. No point looking up the default role.
            return;
        }
        try {
            String query = "latest/meta-data/iam/security-credentials/";
            String uri = "http://" + IAM_ROLE_ENDPOINT + "/" + query;
            String roleName = retrieveRoleFromURI(uri);
            awsConfig.setIamRole(roleName);
        } catch (IOException e) {
            throw new InvalidConfigurationException("Invalid Aws Configuration", e);
        }
    }

    private void getKeysFromIamRole() {
        try {
            String query = "latest/meta-data/iam/security-credentials/" + awsConfig.getIamRole();
            String uri = "http://" + IAM_ROLE_ENDPOINT + "/" + query;
            String json = retrieveRoleFromURI(uri);
            parseAndStoreRoleCreds(json);
        } catch (Exception io) {
            throw new InvalidConfigurationException("Unable to retrieve credentials from IAM Role: "
              + awsConfig.getIamRole(), io);
        }
    }

    /** This helper method is responsible for just parsing the content of the HTTP response and
     * storing the access keys and token it finds there.
     *
     * @param json The JSON representation of the IAM (Task) Role.
     */
    private void parseAndStoreRoleCreds(String json) {
        JsonObject roleAsJson = JsonObject.readFrom(json);
        awsConfig.setAccessKey(roleAsJson.getString("AccessKeyId", null));
        awsConfig.setSecretKey(roleAsJson.getString("SecretAccessKey", null));
        attributes.put("X-Amz-Security-Token", roleAsJson.getString("Token", null));
    }

    /**
     * @deprecated Since we moved JSON parsing from manual pattern matching to using
     * `com.hazelcast.com.eclipsesource.json.JsonObject`, this method should be deprecated.
     * @param reader The reader that gives access to the JSON-formatted content that includes all the role information.
     * @return A map with all the parsed keys and values from the JSON content.
     * @throws IOException In case the input from reader cannot be correctly parsed.
     */
    @Deprecated
    public Map<String, String> parseIamRole(BufferedReader reader) throws IOException {
        Map<String, String> map = new HashMap<String, String>();
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

    private String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat(Constants.DATE_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(new Date());
    }

    public Map<String, String> execute() throws Exception {
        String signature = rs.sign("ec2", attributes);
        Map<String, String> response;
        InputStream stream = null;
        attributes.put("X-Amz-Signature", signature);
        try {
            stream = callService(endpoint);
            response = CloudyUtility.unmarshalTheResponse(stream, awsConfig);
            return response;
        } finally {
            closeResource(stream);
        }
    }

    private InputStream callService(String endpoint) throws Exception {
        String query = rs.getCanonicalizedQueryString(attributes);
        URL url = new URL("https", endpoint, -1, "/?" + query);
        HttpURLConnection httpConnection = (HttpURLConnection) (url.openConnection());
        httpConnection.setRequestMethod(Constants.GET);
        httpConnection.setDoOutput(false);
        httpConnection.connect();
        return httpConnection.getInputStream();
    }
}

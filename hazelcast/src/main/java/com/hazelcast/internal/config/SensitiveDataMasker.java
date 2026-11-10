/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static com.hazelcast.internal.config.LicenseKey.maskLicense;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;

/**
 * Masks sensitive configuration values in output that is only meant to be read,
 * such as logs and the Management Center config view.
 * <p>
 * Never use it for output that is parsed back as configuration (exports,
 * dynamic configuration persistence, client config round-trips): the masked
 * value would replace the real one.
 */
public final class SensitiveDataMasker {

    private static final Pattern SENSITIVE_KEY_PATTERN = Pattern.compile(
            "password|secret|accesskey|privatekey|token|salt|clientid|tenantid|subscriptionid|jaas|user\\.?info");
    private static final Set<String> SENSITIVE_LAST_SEGMENTS = Set.of("credential", "credentials", "identity");
    private static final Pattern URL = Pattern.compile(
            "(jdbc:[a-z0-9]+:(?!//)|(?<![a-z0-9+.-])(?!https?://)[a-z][a-z0-9+.-]*://)\\S+", Pattern.CASE_INSENSITIVE);
    private static final Pattern HTTP_USER_INFO = Pattern.compile("(?<![a-z0-9+.-])(https?://)[^/?#@\\s]+@",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern URL_PARAM = Pattern.compile("((?:^|[?&;])\\s*([^=&;?\\s]+)\\s*=)[^&;\\s]*");
    private static final Pattern SENSITIVE_URL_PARAM_PATTERN = Pattern.compile("pwd|passwd|apikey|accountkey");
    private static final Pattern SYSTEM_PROPERTY_ARG = Pattern.compile("-D([^=]+)=(.*)", Pattern.DOTALL);

    private SensitiveDataMasker() {
    }

    /**
     * Returns the value to show for the given configuration key: the license key
     * is partially masked, values of sensitive keys are fully masked, and in every other
     * value URLs are masked after their scheme, except HTTP(S) URLs which only have their
     * user info masked, and sensitive URL and connection string parameters are masked.
     */
    public static String mask(String key, String value) {
        if (key == null || value == null) {
            return value;
        }
        String normalizedKey = normalize(key);
        if (isLicenseKey(normalizedKey)) {
            // keys from files or secrets often carry a trailing newline, which would
            // otherwise end up in the visible part of the masked key
            return maskLicense(value.strip());
        }
        if (isSensitive(normalizedKey)) {
            return MASK_FOR_SENSITIVE_DATA;
        }
        return maskUrlsAndCredentials(value);
    }


    public static String maskSystemPropertyArg(String arg) {
        if (arg == null) {
            return null;
        }
        Matcher matcher = SYSTEM_PROPERTY_ARG.matcher(arg);
        return matcher.matches() ? "-D" + matcher.group(1) + "=" + mask(matcher.group(1), matcher.group(2)) : arg;
    }

    public static boolean isSensitiveKey(String key) {
        if (key == null) {
            return false;
        }
        String normalizedKey = normalize(key);
        return isLicenseKey(normalizedKey) || isSensitive(normalizedKey);
    }

    private static boolean isSensitive(String normalizedKey) {
        return SENSITIVE_KEY_PATTERN.matcher(normalizedKey).find()
                || SENSITIVE_LAST_SEGMENTS.contains(lastSegment(normalizedKey));
    }

    private static boolean isLicenseKey(String normalizedKey) {
        // hazelcast.licensekey, hazelcast.license-key, hazelcast.enterprise.license.key
        return normalizedKey.endsWith("licensekey") || normalizedKey.endsWith("license.key");
    }

    private static String maskUrlsAndCredentials(String value) {
        if (value.indexOf(':') < 0 && value.indexOf('=') < 0) {
            return value;
        }
        String masked = URL.matcher(value).replaceAll("$1" + MASK_FOR_SENSITIVE_DATA);
        masked = HTTP_USER_INFO.matcher(masked).replaceAll("$1" + MASK_FOR_SENSITIVE_DATA + "@");
        // HTTP(S) query parameters and connection strings without a URL, e.g. Azure's AccountName=...;AccountKey=...
        return URL_PARAM.matcher(masked).replaceAll(param -> Matcher.quoteReplacement(
                isSensitiveUrlParam(param.group(2)) ? param.group(1) + MASK_FOR_SENSITIVE_DATA : param.group()));
    }

    private static boolean isSensitiveUrlParam(String name) {
        String normalizedName = normalize(name).replace("_", "");
        return isSensitive(normalizedName) || SENSITIVE_URL_PARAM_PATTERN.matcher(normalizedName).find();
    }

    private static String lastSegment(String normalizedKey) {
        return normalizedKey.substring(normalizedKey.lastIndexOf('.') + 1);
    }

    private static String normalize(String key) {
        return lowerCaseInternal(key).replace("-", "");
    }
}

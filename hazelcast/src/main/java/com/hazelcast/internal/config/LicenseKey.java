/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Objects;

public class LicenseKey {

    private static final int LICENSE_KEY_VISIBLE_CHAR_COUNT = 5;

    private final String licenseKey;

    public LicenseKey(String licenseKey) {
        this.licenseKey = licenseKey;
    }

    public String getLicenseKey() {
        return licenseKey;
    }

    public String getMaskedLicense() {
        return maskLicense(licenseKey);
    }

    public static String maskLicense(String licenseKey) {
        if (licenseKey.length() > LICENSE_KEY_VISIBLE_CHAR_COUNT) {
            String[] licenceKeyParts = licenseKey.split("#");
            String originalKeyPart = licenceKeyParts[licenceKeyParts.length - 1];
            return originalKeyPart.substring(0, LICENSE_KEY_VISIBLE_CHAR_COUNT) + "*********"
                    + originalKeyPart.substring(originalKeyPart.length() - LICENSE_KEY_VISIBLE_CHAR_COUNT);
        } else {
            return licenseKey;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LicenseKey that = (LicenseKey) o;
        return Objects.equals(licenseKey, that.licenseKey);
    }

    @Override
    public int hashCode() {
        return licenseKey.hashCode();
    }

    @Override
    @SuppressWarnings("checkstyle:MagicNumber")
    public String toString() {
        return "LicenseKey{"
                + "licenseKey='" + getMaskedLicense() + '\''
                + '}';
    }
}

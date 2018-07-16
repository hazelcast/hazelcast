/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;

/**
 * Value object for sending license information to Management Center.
 */
public class LicenseInfo {
    private final int allowedNumberOfNodes;
    private final int type;
    private final String companyName;
    private final String email;
    private final long expiryDate;

    public LicenseInfo(int allowedNumberOfNodes, int type, String companyName, String email, long expiryDate) {
        this.allowedNumberOfNodes = allowedNumberOfNodes;
        this.type = type;
        this.companyName = companyName;
        this.email = email;
        this.expiryDate = expiryDate;
    }

    public int getAllowedNumberOfNodes() {
        return allowedNumberOfNodes;
    }

    public int getType() {
        return type;
    }

    public String getCompanyName() {
        return companyName;
    }

    public String getEmail() {
        return email;
    }

    public long getExpiryDate() {
        return expiryDate;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("allowedNumberOfNodes", allowedNumberOfNodes);
        root.add("type", type);
        root.add("companyName", companyName);
        root.add("email", email);
        root.add("expiryDate", expiryDate);
        return root;
    }
}

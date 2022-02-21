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

package com.hazelcast.internal.cluster.fd;

public enum ClusterFailureDetectorType {

    DEADLINE("deadline"),
    PHI_ACCRUAL("phi-accrual");

    private final String name;

    ClusterFailureDetectorType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Returns the {@code ClusterFailureDetectorType} identified by the given {@code name} or throws an
     * {@link IllegalArgumentException} if none can be identified. In contrast to {@link #valueOf(String)}, this method
     * does not match on the identifier name ("DEADLINE", "PHI_ACCRUAL"), but rather on the explicitly specified
     * {@link #name} field's value.
     *
     * @param name  name of the requested {@code ClusterFailureDetectorType}
     * @return      the {@code ClusterFailureDetectorType} identified by the given name.
     */
    public static ClusterFailureDetectorType of(String name) {
        for (ClusterFailureDetectorType fdType : values()) {
            if (fdType.name.equals(name)) {
                return fdType;
            }
        }
        throw new IllegalArgumentException("Unknown failure detector type: " + name);
    }
}

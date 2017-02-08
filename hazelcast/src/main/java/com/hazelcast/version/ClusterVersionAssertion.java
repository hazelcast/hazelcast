/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.version;

/**
 * Assertion that may be used to verify the ClusterVersion conditions in the production code
 */
public final class ClusterVersionAssertion {

    private final ClusterVersion actual;

    private ClusterVersionAssertion(ClusterVersion clusterVersion) {
        this.actual = clusterVersion;
    }

    public void isLessThan(ClusterVersion version) {
        if (!actual.isLessThan(version)) {
            fail("not less than", version.toString());
        }
    }

    public void isLessOrEqual(ClusterVersion version) {
        if (!actual.isLessOrEqual(version)) {
            fail("not less or equal", version.toString());
        }
    }

    public void isGreaterThan(ClusterVersion version) {
        if (!actual.isGreaterThan(version)) {
            fail("not greater than", version.toString());
        }
    }

    public void isGreaterOrEqual(ClusterVersion version) {
        if (!actual.isGreaterOrEqual(version)) {
            fail("not greater or equal", version.toString());
        }
    }

    public void isEqualTo(ClusterVersion version) {
        if (!actual.isEqualTo(version)) {
            fail("not equal to", version.toString());
        }
    }

    public void isUnknown() {
        if (!actual.isUnknown()) {
            fail("not unknown");
        }
    }

    public void isNotUnknown() {
        if (actual.isUnknown()) {
            fail("unknown");
        }
    }

    private void fail(String msg, String version) {
        throw new IllegalVersionException(
                String.format("Actual cluster version %s is %s %s", actual.toString(), msg, version));
    }

    private void fail(String msg) {
        fail(msg, "");
    }

    public static ClusterVersionAssertion assertThat(ClusterVersion actualClusterVersion) {
        return new ClusterVersionAssertion(actualClusterVersion);
    }
}

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

package com.hazelcast.version;

import com.hazelcast.nio.serialization.SerializableByConvention;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Version comparator that disregards patch version, comparing versions on their major & minor versions only.
 */
@SerializableByConvention
@SuppressWarnings("checkstyle:magicnumber")
class MajorMinorVersionComparator implements Comparator<MemberVersion>, Serializable {

    private static final long serialVersionUID = 364570099633468810L;

    @Override
    public int compare(MemberVersion o1, MemberVersion o2) {
        int thisVersion = (o1.getMajor() << 8 & 0xff00) | (o1.getMinor() & 0xff);
        int thatVersion = (o2.getMajor() << 8 & 0xff00) | (o2.getMinor() & 0xff);
        if (thisVersion > thatVersion) {
            return 1;
        } else {
            return thisVersion == thatVersion ? 0 : -1;
        }
    }
}

/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

public class FormatUtil {

    public static String humanReadableByteCountSI(double bytes) {
        if (Double.isInfinite(bytes)) {
            return "Infinite";
        }

        if (-1000 < bytes && bytes < 1000) {
            return bytes + "B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.2f %cB", bytes / 1000.0, ci.current());
    }

    public static String humanReadableCountSI(double count) {
        if (Double.isInfinite(count)) {
            return "Infinite";
        }

        if (-1000 < count && count < 1000) {
            return String.format("%.2f", count);
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (count <= -999_950 || count >= 999_950) {
            count /= 1000;
            ci.next();
        }
        return String.format("%.2f%c", count / 1000.0, ci.current());
    }

    public static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + "B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.2f %cB", bytes / 1000.0, ci.current());
    }
}

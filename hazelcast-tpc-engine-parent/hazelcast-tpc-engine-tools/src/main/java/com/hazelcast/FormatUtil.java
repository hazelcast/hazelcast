package com.hazelcast;

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
            return String.valueOf(count);
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

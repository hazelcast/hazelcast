package com.hazelcast.sql.tpch.model;

import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Helper methods for substitutions.
 */
public final class Substitution {
    private static final String[] TYPES_1 = new String[] {
        "STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"
    };

    private static final String[] TYPES_2 = new String[] {
        "ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"
    };

    private static final String[] TYPES_3 = new String[] {
        "TIN", "NICKEL", "BRASS", "STEEL", "COPPER"
    };

    private static final String[] REGION_NAMES = new String[] {
        "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"
    };

    private static final String[] SEGMENTS = new String[] {
        "AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"
    };

    private Substitution() {
        // No-op.
    }

    public static LocalDate q1Date() {
        LocalDate date = LocalDate.parse("1998-12-01");

        return date.minusDays(randomInclusive(60, 120));
    }

    public static int q2Size() {
        return randomInclusive(1, 50);
    }

    public static String q2Type() {
        return "%" + random(TYPES_3);
    }

    public static String q2Region() {
        return random(REGION_NAMES);
    }

    public static String q3Segment() {
        return random(SEGMENTS);
    }

    public static LocalDate q3Date() {
        LocalDate date = LocalDate.parse("1995-03-01");

        return date.plusDays(randomInclusive(0, 30));
    }

    private static int randomInclusive(int startInclusive, int endInclusive) {
        return random(startInclusive, endInclusive + 1);
    }

    private static int random(int startInclusive, int endExclusive) {
        return random().nextInt(startInclusive, endExclusive);
    }

    private static String random(String[] values) {
        int idx = random(0, values.length);

        return values[idx];
    }

    private static ThreadLocalRandom random() {
        return ThreadLocalRandom.current();
    }
}

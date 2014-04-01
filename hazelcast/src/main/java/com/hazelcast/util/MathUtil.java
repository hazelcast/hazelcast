package com.hazelcast.util;

/**
 * @author mdogan 04/12/13
 */
public final class MathUtil {

    public static int divideByAndRoundToInt(double d, int k) {
        return (int) Math.rint(d / k);
    }

}

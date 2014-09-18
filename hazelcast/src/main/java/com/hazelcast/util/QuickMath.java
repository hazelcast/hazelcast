package com.hazelcast.util;

/**
 * The class {@code QuickMath} contains methods to perform optimized mathematical operations.
 * Methods are allowed to put additional constraints on the range of input values if required for efficiency.
 * Methods are <b>not</b> required to perform validation of input arguments, but they have to indicate the constraints
 * in theirs contract.
 *
 *
 */
public final class QuickMath {

    private QuickMath() { }

    /**
     * Return true if input argument is power of two.
     * Input has to be a a positive integer.
     *
     * Result is undefined for zero or negative integers.
     *
     * @param x
     * @return <code>true</code> if <code>x</code> is power of two
     */
    public static boolean isPowerOfTwo(int x) {
        return (x & (x - 1)) == 0;
    }

    /**
     * Computes the remainder of the division of <code>a</code> by <code>b</code>.
     * <code>a</code> has to be non-negative integer and <code>b</code> has to be power of two
     * otherwise the result is undefined.
     *
     * @param a
     * @param b
     * @return remainder of the division of a by b.
     */
    public static int mod(int a, int b) {
        return a & (b - 1);
    }
}

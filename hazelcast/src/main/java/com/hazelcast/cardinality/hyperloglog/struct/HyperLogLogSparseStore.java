package com.hazelcast.cardinality.hyperloglog.struct;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * [1] http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 */
public class HyperLogLogSparseStore
        extends AbstractHyperLogLog {

    private static final long P_PRIME_FENCE_MASK = 0x8000000000L;

    private final long P_PRIME = 25; // [1] Shows good cardinality estimation for the sparse range of sets. P_PRIME within [p .. 25]
    private final long P_PRIME_MASK = (1 << P_PRIME) - 1;
    private final long P_MASK;
    private final long P_DIFF_MASK;
    private final long P_DIFF_ENCODED_MASK;
    private final long ENCODED_P_DIFF_FENCE;
    private final long ENCODED_P_DIFF_SHIFT_BITS;

    private final Map<Integer, Integer> sparseSet = new HashMap<Integer, Integer>();
    private final int[] temp;
    private final int defaultTempSetCapacity;
    private final int sparseToDenseThreshold;
    private final int mPrime;
    private int tempIdx = 0;

    public HyperLogLogSparseStore(final IHyperLogLogContext ctx, final int p) {
        super(ctx, p);
        this.mPrime = 1 << P_PRIME;
        this.sparseToDenseThreshold = 40000;
        this.defaultTempSetCapacity = (int) (sparseToDenseThreshold * .25);
        this.temp = new int[this.defaultTempSetCapacity];

        this.P_MASK = ((1 << p) - 1);
        this.P_DIFF_MASK = P_PRIME_MASK ^ P_MASK;
        this.P_DIFF_ENCODED_MASK = ((1L << 32) - 1) ^ ((1L << 32 - (P_PRIME - p)) - 1);
        this.ENCODED_P_DIFF_FENCE = (1<<(P_PRIME-p));
        this.ENCODED_P_DIFF_SHIFT_BITS = (int) (32 - (P_PRIME - p));
    }

    @Override
    public boolean aggregate(long hash) {
        int encoded = encodeHash(hash);
        temp[tempIdx++] = encoded;
//        System.out.println(encoded + " " + Integer.toBinaryString(encoded));
//        System.out.println(extractDenseIndexOf(encoded) + " " + Integer.toBinaryString(extractDenseIndexOf(encoded)));
//        System.out.println(extractDenseNumOfZerosOf(encoded) + " " + Integer.toBinaryString(extractDenseNumOfZerosOf(encoded)));
        boolean isTempAtCapacity = tempIdx == defaultTempSetCapacity;
        if (isTempAtCapacity) {
            mergeAndResetTmp();
            convertToDenseIfNeeded();
        }

        return true;
    }

    @Override
    public long estimate() {
        mergeAndResetTmp();
        convertToDenseIfNeeded();
        return linearCounting(mPrime, mPrime - sparseSet.size());
    }

    private int encodeHash(long hash) {
        int index = (int) (hash & P_PRIME_MASK) << (32 - P_PRIME);
        if ((hash & P_DIFF_MASK) == 0) {
            return index | Long.numberOfTrailingZeros((hash >> P_PRIME) | P_PRIME_FENCE_MASK) | 0x1;
        }

        return index & ~1;
    }

    private int extractDenseIndexOf(long hash) {
        return (int) ((hash >> (32 - P_PRIME)) & P_MASK);
    }

    private byte extractDenseNumOfZerosOf(long hash) {
        if ((hash & 0x1) == 0) {
            int pDiff = (int) (((hash & P_DIFF_ENCODED_MASK) >> ENCODED_P_DIFF_SHIFT_BITS) | ENCODED_P_DIFF_FENCE);
            return (byte) (Integer.numberOfTrailingZeros(pDiff) + 1);
        }

        return (byte) ((hash & ((1 << (32 - P_PRIME)) - 1) >> 1) + (P_PRIME - p) + 1);
    }

    private void mergeAndResetTmp() {
        for (int i = 0; i < tempIdx; i++) {
            int hash = temp[i];
            int sparseIndex = (int) ((hash >> (32 - P_PRIME) & P_PRIME_MASK) & mPrime - 1);
            Integer oldHashValue = sparseSet.get(sparseIndex);
            if (oldHashValue == null) {
                sparseSet.put(sparseIndex, hash);
            } else if (oldHashValue != hash) {
                int oldRunOfTrailingZeros = extractDenseNumOfZerosOf(oldHashValue);
                int newRunOfTrailingZeros = extractDenseNumOfZerosOf(hash);
                if (newRunOfTrailingZeros > oldRunOfTrailingZeros) {
                    sparseSet.put(sparseIndex, hash);
                }
            }
        }

        Arrays.fill(temp, 0);
        tempIdx = 0;
    }

    private void convertToDenseIfNeeded() {
        // For absolute correctness this should be based on the actual mem footprint of the sparse layout vs dense layout.
        boolean shouldConvertToDense = sparseSet.size() > sparseToDenseThreshold;
        if (shouldConvertToDense) {
            switchStore(asDense());
        }
    }

    private IHyperLogLog asDense() {
        byte[] register = new byte[m];
        for (int hash : sparseSet.values()) {
            int index = extractDenseIndexOf(hash) & (register.length - 1);
            register[index] = (byte) Math.max(register[index], extractDenseNumOfZerosOf(hash));
        }

        return new HyperLogLogDenseStore(getContext(), p, register);
    }

}




package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.cardinality.hyperloglog.struct.HyperLogLogEncType;
import com.hazelcast.cardinality.hyperloglog.struct.IHyperLogLog;
import com.hazelcast.cardinality.hyperloglog.struct.IHyperLogLogContext;
import com.hazelcast.util.HashUtil;

import java.util.Random;

import static com.hazelcast.cardinality.hyperloglog.struct.HyperLogLogEncType.SPARSE;

public class HyperLogLogContainer implements IHyperLogLog, IHyperLogLogContext {

    private static final int DEFAULT_HLL_PRECISION = 14;

    private IHyperLogLog store;

    public HyperLogLogContainer(HyperLogLogEncType type) {
        this.store = type.build(this, DEFAULT_HLL_PRECISION);
    }

    @Override
    public void setStore(IHyperLogLog store) {
        this.store = store;
    }

    @Override
    public long estimate() {
        return store.estimate();
    }

    @Override
    public boolean aggregate(long hash) {
        return store.aggregate(hash);
    }

    @Override
    public boolean aggregate(long[] hashes) {
        return store.aggregate(hashes);
    }

    public static void main(String... args) {
        HyperLogLogContainer c = new HyperLogLogContainer(SPARSE);
        Random r = new Random();

        int total = 1500000;

        byte b[] = new byte[120];
        for (int i=1; i<=total; i++) {
            r.nextBytes(b);
            long hash = HashUtil.MurmurHash3_x64_64(b, 0, 120);
//        IHyperLogLog hllD = new HyperLogLogDenseStore(c, DEFAULT_HLL_PRECISION);
//        IHyperLogLog hllS = new HyperLogLogSparseStore(c, DEFAULT_HLL_PRECISION);
//        hllD.aggregate(hash);
//        System.out.println("------");
//        hllS.aggregate(hash);
            c.aggregate(hash);
            long est = c.estimate();
            double pct = 100.0 - ((est * 100.0) / i);
            System.out.println("Estimate: " + est + ". Real: " + i + " Error margin: " + pct);
            if (pct > 1.0)
                break;
//        c.aggregate(1L);
        }
    }
}

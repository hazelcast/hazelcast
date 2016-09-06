package com.hazelcast.cardinality.hyperloglog.struct;

public enum HyperLogLogEncType {

    SPARSE {
        @Override
        public IHyperLogLog build(IHyperLogLogContext hll, int p) {
            return new HyperLogLogSparseStore(hll, p);
        }
    },
    DENSE {
        @Override
        public IHyperLogLog build(IHyperLogLogContext hll, int p) {
            return new HyperLogLogDenseStore(hll, p);
        }
    };

    public abstract IHyperLogLog build(final IHyperLogLogContext hll, final int p);

}

package com.hazelcast.util;

import static com.hazelcast.util.MathUtil.divideByAndRoundToInt;

public enum MemoryUnit {

	BYTES {
        public long convert(long value, MemoryUnit m) {return m.toBytes(value);}
		public long toBytes(long value) {return value;}
		public long toKiloBytes(long value) {return divideByAndRoundToInt(value, K);}
		public long toMegaBytes(long value) {return divideByAndRoundToInt(value, M);}
		public long toGigaBytes(long value) {return divideByAndRoundToInt(value, G);}
	},
	KILOBYTES {
		public long convert(long value, MemoryUnit m) {return m.toKiloBytes(value);}
		public long toBytes(long value) {return value * K;}
		public long toKiloBytes(long value) {return value;}
		public long toMegaBytes(long value) {return divideByAndRoundToInt(value, K);}
		public long toGigaBytes(long value) {return divideByAndRoundToInt(value, M);}
	},
	MEGABYTES {
		public long convert(long value, MemoryUnit m) {return m.toMegaBytes(value);}
		public long toBytes(long value) {return value * M;}
		public long toKiloBytes(long value) {return value * K;}
		public long toMegaBytes(long value) {return value;}
		public long toGigaBytes(long value) {return divideByAndRoundToInt(value, K);}
	},
	GIGABYTES {
		public long convert(long value, MemoryUnit m) {return m.toGigaBytes(value);}
		public long toBytes(long value) {return value * G;}
		public long toKiloBytes(long value) {return value * M;}
		public long toMegaBytes(long value) {return value * K;}
		public long toGigaBytes(long value) {return value;}
	};

	static final int K = 1 << 10;
    static final int M = 1 << 20;
    static final int G = 1 << 30;

	public abstract long convert(long value, MemoryUnit m);
	public abstract long toBytes(long value);
	public abstract long toKiloBytes(long value);
	public abstract long toMegaBytes(long value);
	public abstract long toGigaBytes(long value);
}
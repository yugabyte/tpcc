package com.oltpbenchmark;

/** Efficiently stores a record of (start time, latency) pairs. */
public class TransactionLatencyRecord extends LatencyRecord {

    public TransactionLatencyRecord(long startNs) {
        super(startNs);
    }

    public void addLatency(int transType, long startNs, long endConnectionNs,
                           long operationStartNs, long operationEndNs) {
        assert endConnectionNs >= startNs;
        if (nextIndex == ALLOC_SIZE) {
            allocateChunk();
        }
        Sample[] chunk = (Sample[]) values.get(values.size() - 1);
        int connlatencyUs = toIntMicros(endConnectionNs - startNs);
        int operationLatencyUs = toIntMicros(operationEndNs - operationStartNs);

        chunk[nextIndex] = new Sample(transType, startNs, connlatencyUs, operationLatencyUs);
        ++nextIndex;
    }

    protected LatencyRecord.Sample[] getNewLatencyRecord() {
        return new TransactionLatencyRecord.Sample[ALLOC_SIZE];
    }

    private static int toIntMicros(long durationNs) {
        long us = (durationNs + 500) / 1000;
        if (us < 0) {
            return 0;
        }
        if (us > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) us;
    }

    /** Stores the start time and connection and operation latency for a
     * single sample. Immutable. */
    public static final class Sample extends LatencyRecord.Sample {
        public final int connLatencyUs;
        public final int operationLatencyUs;

        public Sample(int tranType, long startNs, int latencyUs,
                      int operationLatencyUs) {
            super(tranType, startNs);
            this.connLatencyUs = latencyUs;
            this.operationLatencyUs = operationLatencyUs;
        }
    }
}

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
        int connlatencyUs = (int) ((endConnectionNs - startNs + 500) / 1000);
        assert connlatencyUs >= 0;

        int operationLatencyUs = (int)((operationEndNs - operationStartNs + 500) / 1000);
        assert operationLatencyUs >= 0;

        chunk[nextIndex] = new Sample(transType, startNs, connlatencyUs, operationLatencyUs);
        ++nextIndex;
    }

    protected LatencyRecord.Sample[] getNewLatencyRecord() {
        return new TransactionLatencyRecord.Sample[ALLOC_SIZE];
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

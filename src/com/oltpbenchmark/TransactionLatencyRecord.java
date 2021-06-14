package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Iterator;

/** Efficiently stores a record of (start time, latency) pairs. */
public class TransactionLatencyRecord implements Iterable<TransactionLatencyRecord.Sample> {
    /** Allocate space for 200 samples at a time */
    static final int ALLOC_SIZE = 200;

    /**
     * Contains (start time, latency, transactionType, workerid, phaseid) pentiplets
     * in microsecond form.
     */
    private final ArrayList<Sample[]> values = new ArrayList<>();
    private int nextIndex;

    public TransactionLatencyRecord(long startNs) {
        assert startNs > 0;
        allocateChunk();
    }

    public void addLatency(int transType, long startNs, long endConnectionNs,
                           long operationStartNs, long operationEndNs) {
        assert endConnectionNs >= startNs;
        if (nextIndex == ALLOC_SIZE) {
            allocateChunk();
        }
        Sample[] chunk = values.get(values.size() - 1);
        int connlatencyUs = (int) ((endConnectionNs - startNs + 500) / 1000);
        assert connlatencyUs >= 0;

        int operationLatencyUs = (int)((operationEndNs - operationStartNs + 500) / 1000);
        assert operationLatencyUs >= 0;

        chunk[nextIndex] = new Sample(transType, startNs, connlatencyUs, operationLatencyUs);
        ++nextIndex;
    }

    private void allocateChunk() {
        assert (values.isEmpty() && nextIndex == 0)
                || nextIndex == ALLOC_SIZE;
        values.add(new Sample[ALLOC_SIZE]);
        nextIndex = 0;
    }

    /** Returns the number of recorded samples. */
    public int size() {
        // Samples stored in full chunks
        int samples = (values.size() - 1) * ALLOC_SIZE;

        // Samples stored in the last not full chunk
        samples += nextIndex;
        return samples;
    }

    /** Stores the start time and latency for a single sample. Immutable. */
    public static final class Sample implements Comparable<Sample> {
        public final int tranType;
        public final long startNs;
        public final int connLatencyUs;
        public final int operationLatencyUs;

        public Sample(int tranType, long startNs, int latencyUs,
                      int operationLatencyUs) {
            this.tranType = tranType;
            this.startNs = startNs;
            this.connLatencyUs = latencyUs;
            this.operationLatencyUs = operationLatencyUs;
        }

        @Override
        public int compareTo(Sample other) {
            long diff = this.startNs - other.startNs;

            // explicit comparison to avoid long to int overflow
            if (diff > 0)
                return 1;
            else if (diff < 0)
                return -1;
            else {
                return 0;
            }
        }
    }

    private final class TransactionLatencyRecordIterator implements Iterator<Sample> {
        private int chunkIndex = 0;
        private int subIndex = 0;

        @Override
        public boolean hasNext() {
            if (chunkIndex < values.size() - 1) {
                return true;
            }

            assert chunkIndex == values.size() - 1;
            if (subIndex < nextIndex) {
                return true;
            }

            assert chunkIndex == values.size() - 1 && subIndex == nextIndex;
            return false;
        }

        @Override
        public Sample next() {
            Sample[] chunk = values.get(chunkIndex);
            Sample s = chunk[subIndex];

            // Iterate in chunk, and wrap to next one
            ++subIndex;
            assert subIndex <= ALLOC_SIZE;
            if (subIndex == ALLOC_SIZE) {
                chunkIndex += 1;
                subIndex = 0;
            }
            return s;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }
    }

    public Iterator<Sample> iterator() {
        return new TransactionLatencyRecordIterator();
    }
}
/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.util;

import java.util.*;
import java.util.Map.Entry;

/**
 * A very nice and simple generic Histogram
 * @author svelagap
 * @author pavlo
 */
public class Histogram<X extends Comparable<X>> {

    private static final String MARKER = "*";
    private static final Integer MAX_CHARS = 80;
    private static final Integer MAX_VALUE_LENGTH = 20;

    public enum Members {
    }
    
    protected final SortedMap<X, Integer> histogram = new TreeMap<>();
    private transient boolean dirty = false;
    
    /**
     * The Min/Max values are the smallest/greatest values we have seen based
     * on some natural ordering
     */
    protected Comparable<X> min_value;
    protected Comparable<X> max_value;
    
    /**
     * The Min/Max counts are the values that have the smallest/greatest number of
     * occurences in the histogram
     */
    protected int min_count = 0;
    protected List<X> min_count_values;
    protected int max_count = 0;
    protected List<X> max_count_values;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Histogram<?>) {
            Histogram<?> other = (Histogram<?>)obj;
            return (this.histogram.equals(other.histogram));
        }
        return (false);
    }

    /**
     * The main method that updates a value in the histogram with a given sample count
     * This should be called by one of the public interface methods that are synchronized
     * This method is not synchronized on purpose for performance
     */
    private void _put(X value, int count) {
        if (value == null) return;
        
        // If we already have this value in our histogram, then add the new count
        // to its existing total
        if (this.histogram.containsKey(value)) {
            count += this.histogram.get(value);
        }
        assert(count >= 0) : "Invalid negative count for key '" + value + "' [count=" + count + "]";
        // If the new count is zero, then completely remove it if we're not allowed to have zero entries
        this.histogram.put(value, count);
        this.dirty = true;
    }

    /**
     * Recalculate the min/max count value sets
     * Since this is expensive, this should only be done whenever that information is needed 
     */
    private synchronized void calculateInternalValues() {
        if (!this.dirty) return;
        
        // New Min/Max Counts
        // The reason we have to loop through and check every time is that our 
        // value may be the current min/max count and thus it may or may not still
        // be after the count is changed
        this.max_count = 0;
        this.min_count = Integer.MAX_VALUE;
        this.min_value = null;
        this.max_value = null;
        
        if (this.min_count_values == null) this.min_count_values = new ArrayList<>();
        if (this.max_count_values == null) this.max_count_values = new ArrayList<>();
        
        for (Entry<X, Integer> e : this.histogram.entrySet()) {
            X value = e.getKey();
            int cnt = e.getValue();
            
            // Is this value the new min/max values?
            if (this.min_value == null || this.min_value.compareTo(value) > 0) {
                this.min_value = value;
            } else if (this.max_value == null || this.max_value.compareTo(value) < 0) {
                this.max_value = value;
            }
            
            if (cnt <= this.min_count) {
                if (cnt < this.min_count) this.min_count_values.clear();
                this.min_count_values.add(value);
                this.min_count = cnt;
            }
            if (cnt >= this.max_count) {
                if (cnt > this.max_count) this.max_count_values.clear();
                this.max_count_values.add(value);
                this.max_count = cnt;
            }
        } // FOR
        this.dirty = false;
    }

    public boolean isEmpty() {
        return (this.histogram.isEmpty());
    }

    /**
     * Increment multiple values by the given count
     */
    public synchronized void putAll(Collection<X> values, int count) {
        for (X v : values) {
            this._put(v, count);
        } // FOR
    }

    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    /**
     * Histogram Pretty Print
     */
    public String toString() {
        return (this.toString(MAX_CHARS, MAX_VALUE_LENGTH));
    }

    /**
     * Histogram Pretty Print
     */
    public synchronized String toString(Integer max_chars, Integer max_length) {
        StringBuilder s = new StringBuilder();
        if (max_length == null) max_length = MAX_VALUE_LENGTH;
        
        this.calculateInternalValues();
        
        // Figure out the max size of the counts
        int max_ctr_length = 4;
        for (Integer ctr : this.histogram.values()) {
            max_ctr_length = Math.max(max_ctr_length, ctr.toString().length());
        } // FOR
        
        // Don't let anything go longer than MAX_VALUE_LENGTH chars
        String f = "%-" + max_length + "s [%" + max_ctr_length + "d] ";
        boolean first = true;
        for (X value : this.histogram.keySet()) {
            if (!first) s.append("\n");
            String str = (value != null ? value.toString() : "null");
            int value_str_len = str.length();
            if (value_str_len > max_length) str = str.substring(0, max_length - 3) + "...";
            
            int cnt = value != null ? this.histogram.get(value) : 0;
            int chars = (int)((cnt / (double)this.max_count) * max_chars);
            s.append(String.format(f, str, cnt));
            for (int i = 0; i < chars; i++) s.append(MARKER);
            first = false;
        } // FOR
        if (this.histogram.isEmpty()) s.append("<EMPTY>");
        return (s.toString());
    }
}

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
import java.util.concurrent.ThreadLocalRandom;

public class RandomGenerator extends Random {

    /**
     * Constructor
     */
    public RandomGenerator(int seed) {
        super(seed);
    }

    /**
     * Returns a random int value between minimum and maximum (inclusive)
     * @return an int in the range [minimum, maximum]. Note that this is inclusive.
     */
    public int number(int minimum, int maximum) {
        assert minimum <= maximum : String.format("%d <= %d", minimum, maximum);
        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    /**
     * Returns a random int value between minimum and maximum (inclusive).
     * Uses an implementation that will not have the same thread contention
     * issues as the default Random.nextInt() method.
     * @return an int in the range [minimum, maximum]. Note that this is inclusive.
     */
    public int fastNumber(int minimum, int maximum) {
        assert minimum <= maximum : String.format("%d <= %d", minimum, maximum);
        int range_size = maximum - minimum + 1;
        int value = ThreadLocalRandom.current().nextInt(range_size);
        //int value = this.nextInt(range_size);
        value += minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    /** @return a random alphabetic string with length in range [minimum_length, maximum_length].
     */
    public String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'a', 26);
    }


    /** @return a random numeric string with length in range [minimum_length, maximum_length].
     */
    public String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    private String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = fastNumber(minimum_length, maximum_length);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte)(baseByte + fastNumber(0, numCharacters-1));
        }
        return new String(bytes);
    }
}
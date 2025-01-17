/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.stream.membership;

/**
 * The following calculations are taken from:
 * http://www.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 * "Bloom Filters - the math"
 * <p/>
 * This class's static methods are meant to facilitate the use of the Bloom
 * Filter class by helping to choose correct values of 'bits per element' and
 * 'number of hash functions, k'.
 */
public class BloomCalculations {

    private static final int maxBuckets = 19;
    private static final int minBuckets = 2;
    private static final int minK = 1;
    private static final int maxK = 13;
    private static final int[] optKPerBuckets =
            new int[]{1, // dummy K for 0 buckets per element
                      1, // dummy K for 1 buckets per element
                      1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 10, 11, 12, 12, 13, 14, 15, 16, 17, 18, 19, 20};

    /**
     * In the following table, the row 'i' shows false positive rates if i buckets
     * per element are used.  Column 'j' shows false positive rates if j hash
     * functions are used.  The first row is 'i=0', the first column is 'j=0'.
     * Each cell (i,j) the false positive rate determined by using i buckets per
     * element and j hash functions.
     */
    static final double[][] probs = new double[][]{
            {1.0}, // dummy row representing 0 buckets per element
            {1.0, 1.0}, // dummy row representing 1 buckets per element
            {1.0, 0.393, 0.400},
            {1.0, 0.283, 0.237, 0.253},
            {1.0, 0.221, 0.155, 0.147, 0.160},
            {1.0, 0.181, 0.109, 0.092, 0.092, 0.101}, // 5
            {1.0, 0.154, 0.0804, 0.0609, 0.0561, 0.0578, 0.0638},
            {1.0, 0.133, 0.0618, 0.0423, 0.0359, 0.0347, 0.0364},
            {1.0, 0.118, 0.0489, 0.0306, 0.024, 0.0217, 0.0216, 0.0229},
            {1.0, 0.105, 0.0397, 0.0228, 0.0166, 0.0141, 0.0133, 0.0135, 0.0145},
            {1.0, 0.0952, 0.0329, 0.0174, 0.0118, 0.00943, 0.00844, 0.00819, 0.00846}, // 10
            {1.0, 0.0869, 0.0276, 0.0136, 0.00864, 0.0065, 0.00552, 0.00513, 0.00509},
            {1.0, 0.08, 0.0236, 0.0108, 0.00646, 0.00459, 0.00371, 0.00329, 0.00314},
            {1.0, 0.074, 0.0203, 0.00875, 0.00492, 0.00332, 0.00255, 0.00217, 0.00199, 0.00194},
            {1.0, 0.0689, 0.0177, 0.00718, 0.00381, 0.00244, 0.00179, 0.00146, 0.00129, 0.00121, 0.0012},
            {1.0, 0.0645, 0.0156, 0.00596, 0.003, 0.00183, 0.00128, 0.001, 0.000852, 0.000775, 0.000744}, // 15
            {1.0, 0.0606, 0.0138, 0.005, 0.00239, 0.00139, 0.000935, 0.000702, 0.000574, 0.000505, 0.00047, 0.000459},
            {1.0, 0.0571, 0.0123, 0.00423, 0.00193, 0.00107, 0.000692, 0.000499, 0.000394, 0.000335, 0.000302, 0.000287, 0.000284},
            {1.0, 0.054, 0.0111, 0.00362, 0.00158, 0.000839, 0.000519, 0.00036, 0.000275, 0.000226, 0.000198, 0.000183, 0.000176},
            {1.0, 0.0513, 0.00998, 0.00312, 0.0013, 0.000663, 0.000394, 0.000264, 0.000194, 0.000155, 0.000132, 0.000118, 0.000111, 0.000109},
            {1.0, 0.0488, 0.00906, 0.0027, 0.00108, 0.00053, 0.000303, 0.000196, 0.00014, 0.000108, 8.89e-05, 7.77e-05, 7.12e-05, 6.79e-05, 6.71e-05} // 20
    };  // the first column is a dummy column representing K=0.

    /**
     * Given the number of buckets that can be used per element, return the optimal
     * number of hash functions in order to minimize the false positive rate.
     *
     * @param bucketsPerElement
     * @return The number of hash functions that minimize the false positive rate.
     */
    public static int computeBestK(int bucketsPerElement) {
        assert bucketsPerElement >= 0;
        if (bucketsPerElement >= optKPerBuckets.length) {
            return optKPerBuckets[optKPerBuckets.length - 1];
        }
        return optKPerBuckets[bucketsPerElement];
    }

    /**
     * A wrapper class that holds two key parameters for a Bloom Filter: the
     * number of hash functions used, and the number of buckets per element used.
     */
    public static final class BloomSpecification {

        final int K; // number of hash functions.
        final int bucketsPerElement;

        public BloomSpecification(int k, int bucketsPerElement) {
            K = k;
            this.bucketsPerElement = bucketsPerElement;
        }
    }

    /**
     * Given a maximum tolerable false positive probability, compute a Bloom
     * specification which will give less than the specified false positive rate,
     * but minimize the number of buckets per element and the number of hash
     * functions used.  Because bandwidth (and therefore total bitvector size)
     * is considered more expensive than computing power, preference is given
     * to minimizing buckets per element rather than number of hash functions.
     *
     * @param maxFalsePosProb The maximum tolerable false positive rate.
     * @return A Bloom Specification which would result in a false positive rate
     * less than specified by the function call.
     */
    public static BloomSpecification computeBucketsAndK(double maxFalsePosProb) {
        // Handle the trivial cases
        if (maxFalsePosProb >= probs[minBuckets][minK]) {
            return new BloomSpecification(2, optKPerBuckets[2]);
        }
        if (maxFalsePosProb < probs[maxBuckets][maxK]) {
            return new BloomSpecification(maxK, maxBuckets);
        }

        // First find the minimal required number of buckets:
        int bucketsPerElement = 2;
        int K = optKPerBuckets[2];
        while (probs[bucketsPerElement][K] > maxFalsePosProb) {
            bucketsPerElement++;
            K = optKPerBuckets[bucketsPerElement];
        }
        // Now that the number of buckets is sufficient, see if we can relax K
        // without losing too much precision.
        while (probs[bucketsPerElement][K - 1] <= maxFalsePosProb) {
            K--;
        }

        return new BloomSpecification(K, bucketsPerElement);
    }

    /**
     * Calculate the probability of a false positive given the specified
     * number of inserted elements.
     *
     * @param bucketsPerElement number of inserted elements.
     * @param hashCount
     * @return probability of a false positive.
     */
    public static double getFalsePositiveProbability(int bucketsPerElement, int hashCount) {
        // (1 - e^(-k * n / m)) ^ k
        return Math.pow(1 - Math.exp(-hashCount * (1 / (double) bucketsPerElement)), hashCount);

    }
}

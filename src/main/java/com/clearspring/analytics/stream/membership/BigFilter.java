/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.clearspring.analytics.stream.membership;

import com.clearspring.analytics.hash.MurmurHash;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;

public abstract class BigFilter {

    int hashCount;

    public int getHashCount() {
        return hashCount;
    }

    public long[] getHashBuckets(String key) {
        return BigFilter.getHashBuckets(key, hashCount, buckets());
    }

    public long[] getHashBuckets(byte[] key) {
        return BigFilter.getHashBuckets(key, hashCount, buckets());
    }


    abstract long buckets();

    public abstract void add(String key);

    public abstract boolean isPresent(String key);

    // for testing
    abstract int emptyBuckets();

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // https://gnunet.org/sites/default/files/LessHashing2006Kirsch.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    public static long[] getHashBuckets(String key, int hashCount, long max) {
        byte[] b;
        try {
            b = key.getBytes("UTF-16");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return getHashBuckets(b, hashCount, max);
    }

    static long[] getHashBuckets(byte[] b, int hashCount, long max) {
        long[] result = new long[hashCount];
        long hash1 = MurmurHash.hash64(b, b.length, (int)0);
        long hash2 = MurmurHash.hash64(b, b.length, (int)hash1);
        for (int i = 0; i < hashCount; i++) {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }
}

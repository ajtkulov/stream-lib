package com.clearspring.analytics.stream.membership;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;

public class BigBloomFilterSerializer implements ICompactSerializer<BigBloomFilter> {

    public void serialize(BigBloomFilter bf, DataOutputStream dos)
            throws IOException {
        dos.writeInt(bf.getHashCount());
        BigBitSetSerializer.serialize(bf.filter(), dos);
    }

    public BigBloomFilter deserialize(DataInputStream dis) throws IOException {
        int hashes = dis.readInt();
        BigBitSet bs = BigBitSetSerializer.deserialize(dis);
        return new BigBloomFilter(hashes, bs);
    }
}

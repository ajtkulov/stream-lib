package com.clearspring.analytics.stream.membership;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;

public class BloomFilterSerializer implements ICompactSerializer<BloomFilter> {

    public void serialize(BloomFilter bf, DataOutputStream dos)
            throws IOException {
        dos.writeInt(bf.getHashCount());
        BitSetSerializer.serialize(bf.filter(), dos);
    }

    public BloomFilter deserialize(DataInputStream dis) throws IOException {
        int hashes = dis.readInt();
        BitSet bs = BitSetSerializer.deserialize(dis);
        return new BloomFilter(hashes, bs);
    }
}

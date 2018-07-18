package org.apache.ignite.internal.binary.compress;/*
 * Reference Huffman coding
 * Copyright (c) Project Nayuki
 *
 * https://www.nayuki.io/page/reference-huffman-coding
 * https://github.com/nayuki/Reference-Huffman-coding
 */

import org.apache.ignite.internal.binary.BinaryPrimitives;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;

/**
 * Reads from a Huffman-coded bit stream and decodes symbols. Not thread-safe.
 * @see HuffmanEncoder
 */
public final class FastHuffmanDecoder<T> {

    public static final int DEPTH = 14;
    public static final int MASK = (1 << DEPTH) - 1;
    public static final int OFFSET = 32 - DEPTH;

    public static final int INITIAL_LENGTH = GridBinaryMarshaller.TOTAL_LEN_POS + 4;

    public final Node[] lookupTable;

	public FastHuffmanDecoder(CodeTree<T> codeTree) {
		this.lookupTable = new Node[1 << DEPTH];

		for (int i = 0; i < lookupTable.length; i++) {
			Node cur = codeTree.root;
			for (int b = DEPTH - 1; b >= 0; b--) {
				int temp = (i >> b) & 1;

				if (temp == 0)
				    cur = ((InternalNode<Node>)cur).leftChild;
				else
				    cur = ((InternalNode<Node>)cur).rightChild;

				if (cur instanceof Leaf)
				    break;
			}

			lookupTable[i] = cur;
		}
	}

	public byte[] decodeBinaryObject(byte[] input) {
	    int buff = 0;
	    int bpos = 32;
	    int inpos = 0;
	    int outpos = 0;
        byte[] output = new byte[INITIAL_LENGTH];

        // https://stackoverflow.com/questions/28851223/huffman-code-decoder-encoder-in-java-source-generation
        // Should support up to 25 bit sequences
	    while (outpos < output.length) {
	        for (; bpos >= 8; bpos -= 8) {
                buff <<= 8;

                if (inpos < input.length)
                    buff |= 0xff & (int)input[inpos++];
            }

	        Node lookup = lookupTable[(buff >> (OFFSET - bpos)) & MASK];

	        while (!(lookup instanceof Leaf)) {
	            int bit = (buff >> (OFFSET - ++bpos)) & 1;
                if      (bit == 0) lookup = ((InternalNode<Node>)lookup).leftChild;
                else if (bit == 1) lookup = ((InternalNode<Node>)lookup).rightChild;
            }

            Leaf<byte[]> leaf = (Leaf)lookup;

            bpos += Math.min(DEPTH, leaf.code.size());

            int oldpos = outpos;
            if (leaf.symbol.length == 1)
                output[outpos++] = leaf.symbol[0];
            else {
                int length = Math.min(leaf.symbol.length, output.length - outpos);
                System.arraycopy(leaf.symbol, 0, output, outpos, length);
                outpos += length;
            }

            if (outpos == INITIAL_LENGTH) {
                byte[] newOutput = new byte[BinaryPrimitives.readInt(output, GridBinaryMarshaller.TOTAL_LEN_POS)];
                System.arraycopy(output, 0, newOutput, 0, INITIAL_LENGTH);
                output = newOutput;

                if (leaf.symbol.length > 1) {
                    int length = Math.min(leaf.symbol.length, newOutput.length - outpos);
                    System.arraycopy(leaf.symbol, 0, output, oldpos, length);
                    outpos = oldpos + length;
                }
            }
        }

        return output;
	}
}

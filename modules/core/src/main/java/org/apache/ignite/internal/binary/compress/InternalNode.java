package org.apache.ignite.internal.binary.compress;

import java.util.Objects;

/*
 * Reference Huffman coding
 * Copyright (c) Project Nayuki
 *
 * https://www.nayuki.io/page/reference-huffman-coding
 * https://github.com/nayuki/Reference-Huffman-coding
 */


/**
 * An internal node in a code tree. It has two nodes as children. Immutable.
 * @see CodeTree
 */
public final class InternalNode<T> extends Node {

	public final T leftChild;  // Not null

	public final T rightChild;  // Not null



	public InternalNode(T left, T right) {
		Objects.requireNonNull(left);
		Objects.requireNonNull(right);
		leftChild = left;
		rightChild = right;
	}

}

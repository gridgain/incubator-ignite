package org.apache.ignite.internal.binary.compress;/*
 * Reference Huffman coding
 * Copyright (c) Project Nayuki
 *
 * https://www.nayuki.io/page/reference-huffman-coding
 * https://github.com/nayuki/Reference-Huffman-coding
 */

import java.util.List;

/**
 * A leaf node in a code tree. It has a symbol value. Immutable.
 * @see CodeTree
 */
public final class Leaf<T> extends Node {

	public final T symbol;  // Always non-negative
	public final List<Integer> code;

	public Leaf(T sym, List<Integer> c) {
		if (sym == null || c == null || c.isEmpty())
			throw new IllegalArgumentException("Symbol value must be non-negative, code must be correct");
		symbol = sym;
		code = c;
	}

}

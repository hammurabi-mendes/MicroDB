/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.operations;

import java.util.Iterator;

import java.util.function.Function;

public class ConversionIterator<X, Y> implements Iterator<Y> {
	private Iterator<X> iterator;

	private Function<X, Y> convert;

	public ConversionIterator(Iterator<X> iterator, Function<X, Y> convert) {
		this.iterator = iterator;
		
		this.convert = convert;
	}

	public boolean hasNext() {
		return iterator.hasNext();
	}

	public Y next() {
		return convert.apply(iterator.next());
	}
}
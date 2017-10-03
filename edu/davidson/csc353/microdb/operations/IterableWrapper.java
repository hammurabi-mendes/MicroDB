/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.operations;

import java.util.Iterator;

public class IterableWrapper<X> implements Iterable<X> {
	private Iterator<X> iterator;

	public IterableWrapper(Iterator<X> iterator) {
		this.iterator = iterator;
	}

	public Iterator<X> iterator() {
		return iterator;
	}
}

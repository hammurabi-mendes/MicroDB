/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import edu.davidson.csc353.microdb.files.Tuple;

public interface SecondaryIndex<T extends Tuple, K extends Comparable<K>> {
	public Iterable<RecordLocation> get(K key);
}

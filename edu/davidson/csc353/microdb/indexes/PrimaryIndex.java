/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import edu.davidson.csc353.microdb.files.Tuple;

public interface PrimaryIndex<T extends Tuple, K extends Comparable<K>> {
	public RecordLocation get(K key);
}

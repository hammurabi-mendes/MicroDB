/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import java.util.Collection;

import edu.davidson.csc353.microdb.files.Tuple;

public interface MemoryPrimaryIndex<T extends Tuple, K extends Comparable<K>> extends PrimaryIndex<T, K> {
	public boolean add(K key, RecordLocation recordLocation);
	public RecordLocation get(K key);
	public RecordLocation remove(K key);
	
	public Collection<K> allKeys();
	public Collection<K> allKeys(K first);
}

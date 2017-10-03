/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import java.util.List;
import java.util.Collection;

import edu.davidson.csc353.microdb.files.Tuple;

public interface MemorySecondaryIndex<T extends Tuple, K extends Comparable<K>> extends SecondaryIndex<T, K> {
	public void add(K key, RecordLocation recordLocation);
	public Iterable<RecordLocation> get(K key);
	public List<RecordLocation> remove(K key);
	
	public Collection<K> allKeys();
	public Collection<K> allKeys(K first);
}

/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import java.util.HashMap;
import java.util.TreeSet;

import java.util.Collection;

import java.util.function.Function;

import edu.davidson.csc353.microdb.files.*;

public class DensePrimaryIndexTree<T extends Tuple, K extends Comparable<K>> implements MemoryPrimaryIndex<T, K> {
	private TreeSet<K> keys;
	private HashMap<K, RecordLocation> buckets;

	// O(n log(n)) expected
	public DensePrimaryIndexTree(Queriable<T> queriable, Function<T, K> keyExtractor) {
		keys = new TreeSet<>();
		buckets = new HashMap<>();

		for(Record<T> record: queriable) {
			K key = keyExtractor.apply(record.getTuple());

			if(!buckets.containsKey(key)) {
				keys.add(key);
				buckets.put(key, new RecordLocation(record.getBlockNumber(), record.getRecordNumber()));
			}
		}
	}

	// O(log(n)) expected
	public boolean add(K key, RecordLocation recordLocation) {
		RecordLocation previous = get(key);

		if(previous != null) {
			return false;
		}

		// O(log(n))
		keys.add(key);
		// O(1) expected
		buckets.put(key, recordLocation);

		return true;
	}

	// O(1)
	public RecordLocation get(K key) {
		if(!keys.contains(key)) {
			return null;
		}

		return buckets.get(key);
	}

	// O(log(n)) expected
	public RecordLocation remove(K key) {
		RecordLocation previous = get(key);

		if(previous != null) {
			// O(log(n))
			keys.remove(key);
			// O(1) expected
			buckets.remove(key);
		}

		return previous;
	}

	// O(1)
	public Collection<K> allKeys() {
		return keys;
	}

	// O(log(n))
	public Collection<K> allKeys(K first) {
		return keys.tailSet(first, true);
	}
}

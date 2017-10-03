/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import java.util.List;

import java.util.Collection;

import java.util.function.Function;

import edu.davidson.csc353.microdb.files.*;

public class DenseSecondaryIndexTree<T extends Tuple, K extends Comparable<K>> implements MemorySecondaryIndex<T, K> {
	private TreeSet<K> keys;
	private HashMap<K, ArrayList<RecordLocation>> buckets;

	// O(n log(n)) expected
	public DenseSecondaryIndexTree(Queriable<T> queriable, Function<T, K> keyExtractor) {
		keys = new TreeSet<>();
		buckets = new HashMap<>();

		for(Record<T> record: queriable) {
			K key = keyExtractor.apply(record.getTuple());

			if(!buckets.containsKey(key)) {
				keys.add(key);
				buckets.put(key, new ArrayList<>());
			}

			buckets.get(key).add(new RecordLocation(record.getBlockNumber(), record.getRecordNumber()));
		}
	}

	// O(log(n)) expected
	public void add(K key, RecordLocation recordLocation) {
		if(!keys.contains(key)) {
			// O(log(n))
			keys.add(key);

			buckets.put(key, new ArrayList<>());
		}

		// O(1) expected
		buckets.get(key).add(recordLocation);
	}

	// O(1)
	public Iterable<RecordLocation> get(K key) {
		if(!keys.contains(key)) {
			return new ArrayList<RecordLocation>(0);
		}

		return buckets.get(key);
	}

	// O(log(n)) expected
	public List<RecordLocation> remove(K key) {
		if(keys.contains(key)) {
			// O(log(n))
			keys.remove(key);
			// O(1) expected
			return buckets.remove(key);
		}
		else {
			return null;
		}
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

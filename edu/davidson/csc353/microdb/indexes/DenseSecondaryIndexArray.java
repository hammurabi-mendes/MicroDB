/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;

import java.util.Collection;
import java.util.Collections;

import java.util.function.Function;

import edu.davidson.csc353.microdb.files.*;

public class DenseSecondaryIndexArray<T extends Tuple, K extends Comparable<K>> implements MemorySecondaryIndex<T, K> {
	private ArrayList<K> keys;
	private HashMap<K, ArrayList<RecordLocation>> buckets;

	// O(n)
	public DenseSecondaryIndexArray(Queriable<T> queriable, Function<T, K> keyExtractor) {
		keys = new ArrayList<>();
		buckets = new HashMap<>();

		for(Record<T> record: queriable) {
			K key = keyExtractor.apply(record.getTuple());

			if(!buckets.containsKey(key)) {
				keys.add(key);
				buckets.put(key, new ArrayList<>());
			}

			buckets.get(key).add(new RecordLocation(record.getBlockNumber(), record.getRecordNumber()));
		}

		Collections.sort(keys);
	}

	// O(n)
	public void add(K key, RecordLocation recordLocation) {
		// O(log(n))
		int keyIndex = Collections.binarySearch(keys, key);

		if(keyIndex < 0) {
			// O(n)
			keys.add(-(keyIndex + 1), key); // Read documentation on binarySearch

			buckets.put(key, new ArrayList<>());
		}

		// O(1) expected
		buckets.get(key).add(recordLocation);
	}

	// O(log(n)) expected
	public Iterable<RecordLocation> get(K key) {
		// O(log(n))
		int keyIndex = Collections.binarySearch(keys, key);

		if(keyIndex < 0) {
			return new ArrayList<RecordLocation>(0);
		}

		// O(1) expected
		return buckets.get(key);
	}

	// O(n)
	public List<RecordLocation> remove(K key) {
		int keyIndex = Collections.binarySearch(keys, key);

		if(keyIndex > 0) {
			// O(n)
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
		int keyIndex = Collections.binarySearch(keys, first);

		if(keyIndex < 0) {
			return keys.subList(-(keyIndex + 1), keys.size()); // Read documentation on binarySearch
		}
		else {
			return keys.subList(keyIndex, keys.size());
		}	
	}
}

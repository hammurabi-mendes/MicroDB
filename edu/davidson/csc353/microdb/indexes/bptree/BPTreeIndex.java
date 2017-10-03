package edu.davidson.csc353.microdb.indexes.bptree;

import java.util.ArrayList;
import java.util.function.Function;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Queriable;

import edu.davidson.csc353.microdb.indexes.RecordLocation;

import edu.davidson.csc353.microdb.indexes.SecondaryIndex;

public class BPTreeIndex<T extends Tuple, K extends Comparable<K>> implements SecondaryIndex<T, K> {
	BPTree<K, RecordLocation> tree;

	public BPTreeIndex(Queriable<T> queriable, Function<T, K> keyExtractor, Function<String, K> loadKey) {
		tree = new BPTree<>(loadKey, s -> {
			String clean = s.substring(1, s.length() - 1).replace(", ", ",");

			String[] fields = clean.split(",");

			return new RecordLocation(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
		});

		for(Record<T> record: queriable) {
			T tuple = record.getTuple();
			K key = keyExtractor.apply(tuple);

			tree.insert(key, new RecordLocation(record.getBlockNumber(), record.getRecordNumber()));
		}
	}

	public Iterable<RecordLocation> get(K key) {
		ArrayList<RecordLocation> results = new ArrayList<>();

		RecordLocation location = tree.get(key);

		if(location != null) {
			results.add(location);
		}

		return results;
	}
}
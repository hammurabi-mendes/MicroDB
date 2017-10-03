/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.operations;

import java.util.Set;
import java.util.HashSet;

import java.util.Collection;

import java.util.function.Function;
import java.util.function.BiFunction;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.files.Queriable;
import edu.davidson.csc353.microdb.indexes.IndexEntry;
import edu.davidson.csc353.microdb.indexes.MemoryPrimaryIndex;
import edu.davidson.csc353.microdb.indexes.MemorySecondaryIndex;

import edu.davidson.csc353.microdb.indexes.PrimaryIndex;
import edu.davidson.csc353.microdb.indexes.SecondaryIndex;
import edu.davidson.csc353.microdb.sorting.Merger;

public class Joiner<P extends Tuple, Q extends Tuple, R extends Tuple> {
	private Queriable<P> queriable1;
	private Queriable<Q> queriable2;

	private BiFunction<P, Q, R> tupleMaker;

	public Joiner(Queriable<P> queriable1, Queriable<Q> queriable2, BiFunction<P,Q,R> tupleMaker) {
		this.queriable1 = queriable1;
		this.queriable2 = queriable2;

		this.tupleMaker = tupleMaker;
	}

	public Queriable<R> loopJoin(BiFunction<P, Q, Boolean> predicate) {
		Relation<R> relation = new Relation<R>("join", () -> tupleMaker.apply(null, null));
		relation.clear();

		for(Record<P> record1: queriable1) {
			for(Record<Q> record2: queriable2) {
				if(predicate.apply(record1.getTuple(), record2.getTuple())) {
					relation.appendRecord(new Record<R>(tupleMaker.apply(record1.getTuple(), record2.getTuple())));
				}
			}
		}

		return relation;
	}

	public <K extends Comparable<K>> Queriable<R> indexJoin(Function<P, K> keyExtractor1, Function<Q, K> keyExtractor2, PrimaryIndex<Q, K> index2) {
		Relation<R> relation = new Relation<R>("join", () -> tupleMaker.apply(null, null));
		relation.clear();

		for(Record<P> record1: queriable1) {
			Selector<Q> selector2 = new Selector<Q>(queriable2);

			// Only change from SecondaryIndex version is below
			for(Record<Q> record2: selector2.selectMatchingKeys(index2, keyExtractor1.apply(record1.getTuple()), keyExtractor2)) {
				relation.appendRecord(new Record<R>(tupleMaker.apply(record1.getTuple(), record2.getTuple())));
			}
		}

		return relation;
	}

	public <K extends Comparable<K>> Queriable<R> indexJoin(Function<P, K> keyExtractor1, Function<Q, K> keyExtractor2, SecondaryIndex<Q, K> index2) {
		Relation<R> relation = new Relation<R>("join", () -> tupleMaker.apply(null, null));
		relation.clear();

		for(Record<P> record1: queriable1) {
			Selector<Q> selector2 = new Selector<Q>(queriable2);

			// Only change from PrimaryIndex version is below
			for(Record<Q> record2: selector2.selectMatchingKeys(index2, keyExtractor1.apply(record1.getTuple()))) {
				relation.appendRecord(new Record<R>(tupleMaker.apply(record1.getTuple(), record2.getTuple())));
			}
		}

		return relation;
	}

	public <K extends Comparable<K>> Queriable<R> mergeJoin(MemoryPrimaryIndex<P, K> index1, MemoryPrimaryIndex<Q, K> index2, Function<P, K> keyExtractor1, Function<Q, K> keyExtractor2) {
		Collection<K> keys1 = index1.allKeys();
		Collection<K> keys2 = index2.allKeys();

		Set<K> intersection = new HashSet<K>();

		Set<K> keySet1 = new HashSet<K>(keys1);
		Set<K> keySet2 = new HashSet<K>(keys2);

		intersection.addAll(keySet1);
		intersection.retainAll(keySet2);

		Relation<R> relation = new Relation<R>("join", () -> tupleMaker.apply(null, null));
		relation.clear();

		for(K key: intersection) {
			Selector<P> selector1 = new Selector<P>(queriable1);
			Selector<Q> selector2 = new Selector<Q>(queriable2);

			// Only change from SecondaryIndex version is below
			for(Record<P> record1: selector1.selectMatchingKeys(index1, key, keyExtractor1)) {
				for(Record<Q> record2: selector2.selectMatchingKeys(index2, key, keyExtractor2)) {
					relation.appendRecord(new Record<R>(tupleMaker.apply(record1.getTuple(), record2.getTuple())));
				}
			}
		}

		return relation;
	}

	public <K extends Comparable<K>> Queriable<R> mergeJoin(MemorySecondaryIndex<P, K> index1, MemorySecondaryIndex<Q, K> index2) {
		Collection<K> keys1 = index1.allKeys();
		Collection<K> keys2 = index2.allKeys();

		Set<K> intersection = new HashSet<K>();

		Set<K> keySet1 = new HashSet<K>(keys1);
		Set<K> keySet2 = new HashSet<K>(keys2);

		intersection.addAll(keySet1);
		intersection.retainAll(keySet2);

		Relation<R> relation = new Relation<R>("join", () -> tupleMaker.apply(null, null));
		relation.clear();

		for(K key: intersection) {
			Selector<P> selector1 = new Selector<P>(queriable1);
			Selector<Q> selector2 = new Selector<Q>(queriable2);

			// Only change from PrimaryIndex version is below
			for(Record<P> record1: selector1.selectMatchingKeys(index1, key)) {
				for(Record<Q> record2: selector2.selectMatchingKeys(index2, key)) {
					relation.appendRecord(new Record<R>(tupleMaker.apply(record1.getTuple(), record2.getTuple())));
				}
			}
		}

		return relation;
	}

	// Activity 13

	public <K extends Comparable<K>> Queriable<R> mergeJoin(PrimaryIndex<P, K> index1, PrimaryIndex<Q, K> index2, Function<P, K> keyExtractor1, Function<Q, K> keyExtractor2, Function<K, String> saveKey, Function<String, K> loadKey) {
		Relation<IndexEntry> indexEntries1 = new Relation<IndexEntry>("mergejoin1", () -> new IndexEntry(null, 0, 0));
		indexEntries1.clear();

		for(Record<P> record: queriable1) {
			K key = keyExtractor1.apply(record.getTuple());

			indexEntries1.appendRecord(new Record<IndexEntry>(new IndexEntry(saveKey.apply(key), record.getBlockNumber(), record.getRecordNumber())));
		}

		Relation<IndexEntry> indexEntries2 = new Relation<IndexEntry>("mergejoin2", () -> new IndexEntry(null, 0, 0));
		indexEntries2.clear();

		for(Record<Q> record: queriable2) {
			K key = keyExtractor2.apply(record.getTuple());

			indexEntries1.appendRecord(new Record<IndexEntry>(new IndexEntry(saveKey.apply(key), record.getBlockNumber(), record.getRecordNumber())));
		}

		Merger<IndexEntry, String> merger = new Merger<>(indexEntries1, indexEntries2, () -> new IndexEntry(null, 0, 0), t -> t.key);
		merger.setEliminateDuplicates(true);

		Relation<R> relation = new Relation<R>("join", () -> tupleMaker.apply(null, null));
		relation.clear();

		for(Record<IndexEntry> recordIndexEntry: merger.merge()) {
			Selector<P> selector1 = new Selector<P>(queriable1);
			Selector<Q> selector2 = new Selector<Q>(queriable2);

			IndexEntry entry = recordIndexEntry.getTuple();
			K key = loadKey.apply(entry.key);

			// Only change from SecondaryIndex version is below
			for(Record<P> record1: selector1.selectMatchingKeys(index1, key, keyExtractor1)) {
				for(Record<Q> record2: selector2.selectMatchingKeys(index2, key, keyExtractor2)) {
					relation.appendRecord(new Record<R>(tupleMaker.apply(record1.getTuple(), record2.getTuple())));
				}
			}
		}

		return relation;
	}

	public <K extends Comparable<K>> Queriable<R> mergeJoin(SecondaryIndex<P, K> index1, SecondaryIndex<Q, K> index2, Function<P, K> keyExtractor1, Function<Q, K> keyExtractor2, Function<K, String> saveKey, Function<String, K> loadKey) {
		Relation<IndexEntry> indexEntries1 = new Relation<IndexEntry>("mergejoin1", () -> new IndexEntry(null, 0, 0));
		indexEntries1.clear();

		for(Record<P> record: queriable1) {
			K key = keyExtractor1.apply(record.getTuple());

			indexEntries1.appendRecord(new Record<IndexEntry>(new IndexEntry(saveKey.apply(key), record.getBlockNumber(), record.getRecordNumber())));
		}

		Relation<IndexEntry> indexEntries2 = new Relation<IndexEntry>("mergejoin2", () -> new IndexEntry(null, 0, 0));
		indexEntries2.clear();

		for(Record<Q> record: queriable2) {
			K key = keyExtractor2.apply(record.getTuple());

			indexEntries1.appendRecord(new Record<IndexEntry>(new IndexEntry(saveKey.apply(key), record.getBlockNumber(), record.getRecordNumber())));
		}

		Merger<IndexEntry, String> merger = new Merger<>(indexEntries1, indexEntries2, () -> new IndexEntry(null, 0, 0), t -> t.key);
		merger.setEliminateDuplicates(true);

		Relation<R> relation = new Relation<R>("join", () -> tupleMaker.apply(null, null));
		relation.clear();

		for(Record<IndexEntry> recordIndexEntry: merger.merge()) {
			Selector<P> selector1 = new Selector<P>(queriable1);
			Selector<Q> selector2 = new Selector<Q>(queriable2);

			IndexEntry entry = recordIndexEntry.getTuple();
			K key = loadKey.apply(entry.key);

			// Only change from SecondaryIndex version is below
			for(Record<P> record1: selector1.selectMatchingKeys(index1, key)) {
				for(Record<Q> record2: selector2.selectMatchingKeys(index2, key)) {
					relation.appendRecord(new Record<R>(tupleMaker.apply(record1.getTuple(), record2.getTuple())));
				}
			}
		}

		return relation;
	}
}

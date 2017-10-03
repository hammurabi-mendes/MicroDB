/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.operations;

import java.util.function.Function;
import java.util.function.Predicate;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Queriable;

import edu.davidson.csc353.microdb.indexes.RecordLocation;

import edu.davidson.csc353.microdb.indexes.PrimaryIndex;
import edu.davidson.csc353.microdb.indexes.SecondaryIndex;

public class Selector<T extends Tuple> {
	private Queriable<T> queriable;

	public Selector(Queriable<T> queriable) {
		this.queriable = queriable;
	}

	public <K extends Comparable<K>> Record<T> selectOne(PrimaryIndex<T, K> index, K key) {
		RecordLocation location = index.get(key);

		return queriable.get(location.getBlockNumber(), location.getRecordNumber());
	}

	public Queriable<T> selectPredicate(Predicate<T> predicate, boolean abortOnFalse) {
		RecordLocation first = new RecordLocation(0, 0);

		return new ResultSet<T>(queriable, first, predicate, abortOnFalse);
	}

	public <K extends Comparable<K>> Queriable<T> selectAll(PrimaryIndex<T, K> index, K key) {
		RecordLocation first = new RecordLocation(0, 0);

		return new ResultSet<T>(queriable, first, (t) -> true, true);
	}

	public <K extends Comparable<K>> Queriable<T> selectFollowingPredicate(PrimaryIndex<T, K> index, K key, Predicate<T> predicate, boolean abortOnFalse) {
		return new ResultSet<T>(queriable, index.get(key), predicate, abortOnFalse);
	}

	public <K extends Comparable<K>> Queriable<T> selectFollowingAll(PrimaryIndex<T, K> index, K key) {
		return new ResultSet<T>(queriable, index.get(key), (t) -> true, true);
	}
	
	public <K extends Comparable<K>> Queriable<T> selectMatchingKeys(PrimaryIndex<T, K> index, K key, Function<T, K> keyExtractor) {
		return new ResultSet<T>(queriable, index.get(key), (t) -> keyExtractor.apply(t).equals(key), true);
	}

	public <K extends Comparable<K>> Queriable<T> selectMatchingKeys(SecondaryIndex<T, K> index, K key) {
		return new ResultSet<T>(queriable, index.get(key));
	}
}

/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.sorting;

import java.util.Iterator;

import java.util.function.Function;
import java.util.function.Supplier;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.files.Queriable;

public class Merger<T extends Tuple, K extends Comparable<K>> {
	private Queriable<T> queriable1;
	private Queriable<T> queriable2;

	private Supplier<T> tupleMaker;
	private Function<T, K> keyExtractor;

	private boolean eliminateDuplicates;

	public Merger(Queriable<T> queriable1, Queriable<T> queriable2, Supplier<T> tupleMaker, Function<T, K> keyExtractor) {
		this.queriable1 = queriable1;
		this.queriable2 = queriable2;

		this.tupleMaker = tupleMaker;
		this.keyExtractor = keyExtractor;

		this.eliminateDuplicates = false;
	}

	public void setEliminateDuplicates(boolean eliminateDuplicates) {
		this.eliminateDuplicates = eliminateDuplicates;
	}

	public Queriable<T> merge() {
		Relation<T> result = new Relation<T>("merge", tupleMaker);
		result.clear();

		merge(queriable1, queriable2, result, keyExtractor, eliminateDuplicates);

		return result;
	}

	public static <T extends Tuple, K extends Comparable<K>> void merge(Queriable<T> relation1, Queriable<T> relation2, Relation<T> relation3, Function<T, K> keyExtractor, boolean eliminateDuplicates) {
		Iterator<Record<T>> iterator1 = relation1.iterator();
		Iterator<Record<T>> iterator2 = relation2.iterator();

		Record<T> r1 = null;
		Record<T> r2 = null;

		K key1 = null;
		K key2 = null;

		K keyLastAppended = null;

		while(iterator1.hasNext() && iterator2.hasNext()) {
			if(r1 == null) {
				r1 = iterator1.next();
			}

			if(r2 == null) {
				r2 = iterator2.next();
			}

			key1 = keyExtractor.apply(r1.getTuple());
			key2 = keyExtractor.apply(r2.getTuple());

			if(key1.compareTo(key2) < 0) {
				if(!eliminateDuplicates || (keyLastAppended == null || key1.compareTo(keyLastAppended) != 0)) {
					relation3.appendRecord(r1);
					keyLastAppended = key1;
				}
				r1 = null;
			}
			else if(key1.compareTo(key2) > 0) {
				if(!eliminateDuplicates || (keyLastAppended == null || key2.compareTo(keyLastAppended) != 0)) {
					relation3.appendRecord(r2);
					keyLastAppended = key2;
				}
				r2 = null;
			}
			else {
				if(!eliminateDuplicates || (keyLastAppended == null || key1.compareTo(keyLastAppended) != 0)) {
					relation3.appendRecord(r1);
					keyLastAppended = key1;
				}
				r1 = null;
				r2 = null;
			}
		}

		if(r1 != null) {
			if(!eliminateDuplicates || (keyLastAppended == null || key1.compareTo(keyLastAppended) != 0)) {
				relation3.appendRecord(r1);
				keyLastAppended = key1;
			}
		}

		if(r2 != null) {
			if(!eliminateDuplicates || (keyLastAppended == null || key2.compareTo(keyLastAppended) != 0)) {
				relation3.appendRecord(r2);
				keyLastAppended = key2;
			}
		}

		while(iterator1.hasNext()) {
			r1 = iterator1.next();
			key1 = keyExtractor.apply(r1.getTuple());

			if(!eliminateDuplicates || (keyLastAppended == null || key1.compareTo(keyLastAppended) != 0)) {
				relation3.appendRecord(r1);
				keyLastAppended = key1;
			}
		}

		while(iterator2.hasNext()) {
			r2 = iterator1.next();
			key2 = keyExtractor.apply(r1.getTuple());

			if(!eliminateDuplicates || (keyLastAppended == null || key2.compareTo(keyLastAppended) != 0)) {
				relation3.appendRecord(r2);
				keyLastAppended = key2;
			}
		}
	}
}

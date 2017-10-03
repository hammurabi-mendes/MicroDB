/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.sorting;

import java.util.ArrayList;

import java.util.function.Function;
import java.util.function.Supplier;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.files.Queriable;

public class Sorter<T extends Tuple, K extends Comparable<K>> {
	public static final int BUFFER_SIZE = 1024;

	private Queriable<T> queriable;
	private Supplier<T> tupleMaker;
	private Function<T, K> keyExtractor;

	private boolean eliminateDuplicates;

	public Sorter(Queriable<T> queriable, Supplier<T> tupleMaker, Function<T, K> keyExtractor) {
		this.queriable = queriable;
		this.tupleMaker = tupleMaker;
		this.keyExtractor = keyExtractor;

		this.eliminateDuplicates = false;
	}

	public void setEliminateDuplicates(boolean eliminateDuplicates) {
		this.eliminateDuplicates = eliminateDuplicates;
	}

	public Queriable<T> sort() {
		int lastRun = 0;

		lastRun = makeRuns();
		lastRun = makeMerges(lastRun);

		Relation<T> relation;
		
		relation = new Relation<T>("run" + lastRun, tupleMaker);
		relation.rename("sorted");

		relation = new Relation<T>("sorted", tupleMaker);

		return relation;
	}

	private int makeRuns() {
		ArrayList<Record<T>> buffer = new ArrayList<>(BUFFER_SIZE);

		int lastRun = 0;

		for(Record<T> record: queriable) {
			if(buffer.size() < BUFFER_SIZE) {
				buffer.add(record);
				continue;
			}

			sortAndSave(buffer, lastRun);

			buffer.clear();
			buffer.add(record);

			lastRun++;
		}

		sortAndSave(buffer, lastRun);

		return lastRun;
	}

	private void sortAndSave(ArrayList<Record<T>> buffer, int run) {
		buffer.sort((r1, r2) -> {
			K key1 = keyExtractor.apply(r1.getTuple());
			K key2 = keyExtractor.apply(r2.getTuple());

			return key1.compareTo(key2);
		});

		Relation<T> currentRun = new Relation<T>("run" + run, tupleMaker);
		currentRun.clear();

		for(Record<T> sortedRecord: buffer) {
			currentRun.appendRecord(sortedRecord);
		}

		currentRun.save();
	}

	private int makeMerges(int lastRun) {
		if(lastRun == 0) {
			return 0;
		}

		int currentRun = lastRun + 1;

		for(int i = 0; i <= currentRun - 2; i += 2) {
			Relation<T> relation1 = new Relation<T>("run" + i, tupleMaker);
			Relation<T> relation2 = new Relation<T>("run" + (i + 1), tupleMaker);

			Relation<T> relation3 = new Relation<T>("run" + currentRun, tupleMaker);
			relation3.clear();

			Merger.merge(relation1, relation2, relation3, keyExtractor, eliminateDuplicates);

			relation1.delete();
			relation2.delete();

			relation3.save();
			relation3.close();
			currentRun++;
		}

		return currentRun - 1;
	}

	public Queriable<T> merge(Queriable<T> relation1, Queriable<T> relation2) {
		Relation<T> result = new Relation<T>("merge", tupleMaker);
		result.clear();

		Merger.merge(relation1, relation2, result, keyExtractor, eliminateDuplicates);

		return result;
	}

	/* FOR SKELETON CODE
	private void merge(Queriable<T> relation1, Queriable<T> relation2, Relation<T> relation3) {
		Iterator<Record<T>> iterator1 = relation1.iterator();
		Iterator<Record<T>> iterator2 = relation2.iterator();

		Record<T> r1 = null;
		Record<T> r2 = null;

		while(iterator1.hasNext() && iterator2.hasNext()) {
			if(r1 == null) {
				r1 = iterator1.next();
			}

			if(r2 == null) {
				r2 = iterator2.next();
			}

			K key1 = keyExtractor.apply(r1.getTuple());
			K key2 = keyExtractor.apply(r2.getTuple());

			if(key1.compareTo(key2) < 0) {
				relation3.appendRecord(r1);
				r1 = null;
			}
			else if(key1.compareTo(key2) > 0) {
				relation3.appendRecord(r2);
				r2 = null;
			}
			else {
				relation3.appendRecord(r1);
				r1 = null;
				relation3.appendRecord(r2);
				r2 = null;
			}
		}

		if(r1 != null) {
			relation3.appendRecord(r1);
		}

		if(r2 != null) {
			relation3.appendRecord(r2);
		}

		while(iterator1.hasNext()) {
			relation3.appendRecord(iterator1.next());
		}

		while(iterator2.hasNext()) {
			relation3.appendRecord(iterator2.next());
		}
	}*/
}

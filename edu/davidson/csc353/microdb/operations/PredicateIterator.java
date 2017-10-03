/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.operations;

import java.util.Iterator;

import java.util.function.Predicate;

import edu.davidson.csc353.microdb.files.Queriable;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.indexes.RecordLocation;

public class PredicateIterator<T extends Tuple> implements Iterator<Record<T>> {
	private Iterator<Record<T>> iterator;
	private Predicate<T> predicate;
	private boolean abortOnFalse;

	private Record<T> current;

	public PredicateIterator(Queriable<T> relation, RecordLocation first, Predicate<T> predicate, boolean abortOnFalse) {
		this.iterator = relation.iterator(first.getBlockNumber(), first.getRecordNumber());
		this.predicate = predicate;
		this.abortOnFalse = abortOnFalse;

		current = null;
		next();
	}

	public boolean hasNext() {
		return (current != null);
	}

	public Record<T> next() {
		Record<T> next = current;

		while(iterator.hasNext()) {
			current = iterator.next();

			if(predicate.test(current.getTuple())) {
				return next;
			}

			if(abortOnFalse) {
				break;
			}
		}

		current = null;

		return next;
	}
}
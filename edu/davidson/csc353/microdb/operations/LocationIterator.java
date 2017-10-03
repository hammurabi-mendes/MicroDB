/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.operations;

import java.util.Iterator;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;

import edu.davidson.csc353.microdb.indexes.RecordLocation;

import edu.davidson.csc353.microdb.files.Queriable;

public class LocationIterator<T extends Tuple> implements Iterator<Record<T>> {
	private Queriable<T> base;
	private Iterator<RecordLocation> recordIterator;

	public LocationIterator(Queriable<T> base, Iterable<RecordLocation> records) {
		this.base = base;
		this.recordIterator = records.iterator();
	}

	public boolean hasNext() {
		return recordIterator.hasNext();
	}

	public Record<T> next() {
		RecordLocation nextLocation = recordIterator.next();

		return base.get(nextLocation.getBlockNumber(), nextLocation.getRecordNumber());
	}
}

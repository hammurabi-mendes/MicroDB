/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.operations;

import java.util.Iterator;
import java.util.function.Predicate;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Block;

import edu.davidson.csc353.microdb.indexes.RecordLocation;

import edu.davidson.csc353.microdb.files.Queriable;

public class ResultSet<T extends Tuple> implements Queriable<T> {
	private Queriable<T> base;
	private Iterator<Record<T>> iterator;

	public ResultSet(Queriable<T> base, Iterable<RecordLocation> records) {
		this.base = base;
		this.iterator = new LocationIterator<T>(base, records);
	}

	public ResultSet(Queriable<T> base, RecordLocation first, Predicate<T> predicate, boolean abortOnFalse) {
		this.base = base;
		this.iterator = new PredicateIterator<T>(base, first, predicate, abortOnFalse);
	}

	public Iterator<Record<T>> iterator() {
		return iterator;
	}

	public int getNumberBlocks() {
		return base.getNumberBlocks();
	}

	public Block<T> get(int blockNumber) {
		return base.get(blockNumber);
	}

	public Record<T> get(int blockNumber, int recordNumber) {
		return base.get(blockNumber, recordNumber);
	}

	public Iterator<Record<T>> iterator(int blockNumber, int recordNumber) {
		return base.iterator(blockNumber, recordNumber);
	}
}

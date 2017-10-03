/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.files;

public class Record<T extends Tuple> {
	private int blockNumber;
	private int recordNumber;

	private T tuple;

	public Record(T tuple) {
		this.blockNumber = -1;
		this.recordNumber = -1;
		
		this.tuple = tuple;
	}

	public Record(int blockNumber, int recordNumber, T tuple) {
		this.blockNumber = blockNumber;
		this.recordNumber = recordNumber;
		
		this.tuple = tuple;
	}

	public int getBlockNumber() {
		return blockNumber;
	}

	public int getRecordNumber() {
		return recordNumber;
	}

	public void setBlockNumber(int blockNumber) {
		this.blockNumber = blockNumber;
	}

	public void setRecordNumber(int recordNumber) {
		this.recordNumber = recordNumber;
	}

	public T getTuple() {
		return tuple;
	}

	public int getSize() {
		return tuple.getSize();
	}

	public void load(String input) {
		tuple.load(input);
	}

	public String save() {
		return tuple.save();
	}
	
	public String toString() {
		return "(" + blockNumber + ", " + recordNumber + ") " + tuple.toString();
	}
}

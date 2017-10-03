/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

public class RecordLocation {
	private int blockNumber;
	private int recordNumber;

	public RecordLocation(int blockNumber, int recordNumber) {
		this.blockNumber = blockNumber;
		this.recordNumber = recordNumber;
	}

	public int getBlockNumber() {
		return blockNumber;
	}

	public int getRecordNumber() {
		return recordNumber;
	}
	
	public String toString() {
		return "(" + blockNumber + ", " + recordNumber + ")";
	}
}

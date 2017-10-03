/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.files;

import java.util.Iterator;

public interface Queriable<T extends Tuple> extends Iterable<Record<T>>{
	public int getNumberBlocks();

	public Block<T> get(int blockNumber);
	public Record<T> get(int blockNumber, int recordNumber);

	public Iterator<Record<T>> iterator();
	public Iterator<Record<T>> iterator(int blockNumber, int recordNumber);
}

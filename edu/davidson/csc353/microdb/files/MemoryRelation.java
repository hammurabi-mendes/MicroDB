/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.files;

import java.util.ArrayList;

import java.util.Iterator;

import java.util.NoSuchElementException;

public class MemoryRelation<T extends Tuple> implements Queriable<T> {
	private ArrayList<Block<T>> blocks;

	public MemoryRelation() {
		this.blocks = new ArrayList<Block<T>>();
	}

	private Block<T> createBlock() {
		Block<T> block = new Block<T>(blocks.size());
		
		blocks.add(block);

		return block;
	}

	public void appendRecord(Record<T> record) {
		Block<T> lastBlock;

		if(blocks.size() == 0) {
			lastBlock = createBlock();
		}
		else {
			lastBlock = blocks.get(blocks.size() - 1);
		}

		if(!lastBlock.canAddRecord(record)) {
			lastBlock = createBlock();
		}

		if(!lastBlock.canAddRecord(record)) {
			throw new RuntimeException("Empty block does not have space for new record. Record = " + record);
		}	
		
		lastBlock.addRecord(record);
	}

	public int getNumberBlocks() {
		return blocks.size();
	}

	public Block<T> get(int blockNumber) {
		return blocks.get(blockNumber);
	}

	public Record<T> get(int blockNumber, int recordNumber) {
		return blocks.get(blockNumber).getRecord(recordNumber);
	}

	public Iterator<Record<T>> iterator() {
		return new RecordIterator(this, 0, 0);
	}

	public Iterator<Record<T>> iterator(int blockNumber, int recordNumber) {
		return new RecordIterator(this, blockNumber, recordNumber);
	}
	
	private class RecordIterator implements Iterator<Record<T>> {
		private MemoryRelation<T> relation;
		private int blockNumber;

		private Iterator<Record<T>> recordIterator;

		public RecordIterator(MemoryRelation<T> relation, int blockNumber, int recordNumber) {
			this.relation = relation;
			this.blockNumber = blockNumber;

			if(blockNumber < relation.blocks.size()) {
				recordIterator = relation.blocks.get(blockNumber).iterator(recordNumber);	
			}
		}

		public boolean hasNext() {
			return (recordIterator != null && recordIterator.hasNext());
		}

		public Record<T> next() {
			if(this.hasNext()) {
				Record<T> next = recordIterator.next();

				if(!recordIterator.hasNext()) {
					blockNumber++;

					if(blockNumber < relation.blocks.size()) {
						recordIterator = relation.blocks.get(blockNumber).iterator();	
					}
					else {
						recordIterator = null;
					}
				}

				return next;
			}

			throw new NoSuchElementException();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}

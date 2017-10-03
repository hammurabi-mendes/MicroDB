/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.files;

import java.nio.ByteBuffer;

import java.util.ArrayList;

import java.util.Iterator;
import java.util.Collection;

import java.util.function.Supplier;

public class Block<T extends Tuple> implements Iterable<Record<T>> {
	public static int SIZE = 64;

	private ByteBuffer buffer;
	private int blockNumber;

	private ArrayList<Record<T>> records;

	private int freeSize;

	public Block(int blockNumber) {
		this.buffer = ByteBuffer.allocateDirect(Block.SIZE);
		this.blockNumber = blockNumber;

		records = new ArrayList<Record<T>>();

		freeSize = Block.SIZE - Integer.BYTES;
	}

	public void loadBuffer(Supplier<T> tupleMaker) {
		buffer.rewind();

		freeSize = Block.SIZE;

		// 1: The number of records
		int numberRecords = buffer.getInt();
		freeSize -= Integer.BYTES;

		// 2: The size for each record
		ArrayList<Integer> sizes = new ArrayList<>();

		for(int i = 0; i < numberRecords; i++) {
			sizes.add(buffer.getInt());
		}

		freeSize -= (Integer.BYTES * numberRecords);

		// 3: The records are written backwards from the end of the file,
		//    with their corresponding size
		records.ensureCapacity(numberRecords);

		int currentSize = 0;
		int totalOffset = 0;

		for(int i = 0; i < numberRecords; i++) {
			currentSize = sizes.get(i);
			totalOffset += currentSize;

			byte[] data = new byte[currentSize];

			buffer.position(Block.SIZE - totalOffset);
			buffer.get(data);

			T tuple = tupleMaker.get();
			tuple.load(new String(data));

			records.add(new Record<T>(blockNumber, i, tuple));

			freeSize -= currentSize;
		}
	}

	public void saveBuffer() {
		buffer.rewind();

		// 1: The number of records
		int numberRecords = records.size();
		buffer.putInt(numberRecords);

		// 2: The size for each record
		ArrayList<Integer> sizes = new ArrayList<>();

		for(int i = 0; i < numberRecords; i++) {
			sizes.add(records.get(i).getSize());
		}

		for(int i = 0; i < numberRecords; i++) {
			buffer.putInt(sizes.get(i));
		}

		// 3: The records are written backwards from the end of the file,
		//    with their corresponding size
		int currentSize = 0;
		int totalOffset = 0;

		for(int i = 0; i < records.size(); i++) {
			currentSize = sizes.get(i);
			totalOffset += currentSize;

			buffer.position(Block.SIZE - totalOffset);
			buffer.put(records.get(i).save().getBytes());
		}
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	public int getBlockNumber() {
		return blockNumber;
	}
	
	public void setBlockNumber(int blockNumber) {
		this.blockNumber = blockNumber;
		
		for(Record<T> record: records) {
			record.setBlockNumber(blockNumber);
		}
	}

	public Collection<Record<T>> getRecords() {
		return records;
	}

	public boolean canAddRecord(Record<T> record) {
		// Account for integer describing the record size in the block header
		return (freeSize >= (record.getSize() + Integer.BYTES));
	}

	public boolean addRecord(Record<T> record) {
		if(canAddRecord(record)) {
			records.add(record);

			record.setBlockNumber(blockNumber);
			record.setRecordNumber(records.size() - 1);

			// Account for integer describing the record size in the block header
			freeSize -= (record.getSize() + Integer.BYTES);

			return true;
		}

		return false;
	}

	public Record<T> getRecord(int position) {
		return records.get(position);
	}

	public void setRecord(Record<T> record, int position) {
		freeSize += records.get(position).getSize();

		records.set(position, record);

		freeSize -= records.get(position).getSize();
	}

	public boolean deleteRecord(Record<T> record) {
		int position = records.indexOf(record);

		return deleteRecord(position);
	}

	public boolean deleteRecord(int position) {
		if(position < 0 || position >= records.size()) {
			return false;
		}

		freeSize += (records.get(position).getSize() + Integer.BYTES);

		records.remove(position);

		return true;
	}

	public Iterator<Record<T>> iterator() {
		return records.iterator();
	}

	public Iterator<Record<T>> iterator(int position) {
		return records.subList(position, records.size()).iterator();
	}
}

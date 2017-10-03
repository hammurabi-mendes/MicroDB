/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.files;

import java.io.RandomAccessFile;

import java.io.BufferedReader;
import java.io.FileReader;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;

import java.nio.file.Files;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import java.util.function.Supplier;
import java.util.function.Function;

public class Relation<T extends Tuple> implements Queriable<T> {
	private String relationName;
	private Supplier<T> tupleMaker;

	private RandomAccessFile relationFile;
	private FileChannel relationChannel;

	private BlockManager<T> blockManager;
	private int numberBlocks;

	public Relation(String relationName, Supplier<T> tupleMaker) {
		this.relationName = relationName;
		this.tupleMaker = tupleMaker;

		blockManager = new BlockManager<T>(this);

		try {
			relationFile = new RandomAccessFile(relationName + ".db", "rws");
			relationChannel = relationFile.getChannel();

			numberBlocks = (int) (relationChannel.size() / Block.SIZE);
		}
		catch (FileNotFoundException exception) {
			// Ignore: a new file has been created
		}
		catch(IOException exception) {
			throw new RuntimeException("Error accessing " + relationName);
		}
	}

	public Block<T> createBlock() {
		Block<T> block = new Block<T>(numberBlocks);

		numberBlocks++;

		blockManager.put(block.getBlockNumber(), block);
		return block;
	}
	
	public void deleteBlock(int blockNumber) {
		Block<T> lastBlock = readBlock(numberBlocks - 1);

		lastBlock.setBlockNumber(blockNumber);
		numberBlocks--;

		blockManager.put(blockNumber, lastBlock);
	}

	private Block<T> readBlock(int blockNumber) {
		if(blockManager.contains(blockNumber)) {
			return blockManager.get(blockNumber);
		}

		Block<T> block = new Block<T>(blockNumber);

		try {
			relationChannel.read(block.getBuffer(), blockNumber * Block.SIZE);

			block.loadBuffer(tupleMaker);
		}
		catch (IOException e) {
			throw new RuntimeException("Error accessing " + blockNumber + " on file " + relationName);
		}

		blockManager.put(blockNumber, block);
		return block;
	}

	private void writeBlock(int blockNumber) {
		if(blockManager.contains(blockNumber)) {
			writeBlock(blockManager.get(blockNumber));
		}
	}

	public void writeBlock(Block<T> block) {
		block.saveBuffer();

		ByteBuffer blockBuffer = block.getBuffer();
		int blockNumber = block.getBlockNumber();

		try {
			blockBuffer.rewind();
			relationChannel.write(blockBuffer, blockNumber * Block.SIZE);
		}
		catch (IOException e) {
			throw new RuntimeException("Error accessing " + blockNumber + " on file " + relationName);
		}
	}

	public void appendRecord(Record<T> record) {
		Block<T> lastBlock;

		if(numberBlocks == 0) {
			lastBlock = createBlock();
		}
		else {
			lastBlock = readBlock(numberBlocks - 1);
		}

		if(!lastBlock.canAddRecord(record)) {
			lastBlock = createBlock();
		}

		if(!lastBlock.canAddRecord(record)) {
			throw new RuntimeException("Empty block does not have space for new record. Record = " + record);
		}	

		lastBlock.addRecord(record);
	}

	public void load() {
		for(int i = 0; i < numberBlocks; i++) {
			readBlock(i);
		}
	}

	public void save() {
		for(int i = 0; i < numberBlocks; i++) {
			writeBlock(i);
		}
	}

	public void clear() {
		try {
			relationChannel.truncate(0);
			numberBlocks = 0;
		}
		catch(IOException exception) {
			throw new RuntimeException("Error removing database file " +relationName + ".db");
		}
	}
	
	public void close() {
		try {
			relationChannel.close();
		}
		catch(IOException exception) {
			throw new RuntimeException("Error removing database file " +relationName + ".db");
		}
	}

	public void rename(String newRelationName) {
		try {
			relationChannel.close();
			Files.move(Paths.get(relationName + ".db"), Paths.get(newRelationName + ".db"), REPLACE_EXISTING);
		}
		catch(IOException exception) {
			//throw new RuntimeException("Error removing database file " +relationName + ".db");
			exception.printStackTrace();
		}		
	}
	
	public void delete() {
		try {
			relationChannel.close();
			Files.delete(Paths.get(relationName + ".db"));
		}
		catch(IOException exception) {
			//throw new RuntimeException("Error removing database file " +relationName + ".db");
			exception.printStackTrace();
		}		
	}

	public int getNumberBlocks() {
		return numberBlocks;
	}

	public Block<T> get(int blockNumber) {
		return readBlock(blockNumber);
	}

	public Record<T> get(int blockNumber, int recordNumber) {
		return readBlock(blockNumber).getRecord(recordNumber);
	}

	public Iterator<Record<T>> iterator() {
		return new RecordIterator(this, 0, 0);
	}

	public Iterator<Record<T>> iterator(int blockNumber, int recordNumber) {
		return new RecordIterator(this, blockNumber, recordNumber);
	}

	private class RecordIterator implements Iterator<Record<T>> {
		private Relation<T> relation;
		private int blockNumber;

		private Iterator<Record<T>> recordIterator;

		public RecordIterator(Relation<T> relation, int blockNumber, int recordNumber) {
			this.relation = relation;
			this.blockNumber = blockNumber;

			if(blockNumber < relation.numberBlocks) {
				recordIterator = relation.readBlock(blockNumber).iterator(recordNumber);	
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

					if(blockNumber < relation.numberBlocks) {
						recordIterator = relation.readBlock(blockNumber).iterator();	
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

	public void importFromFile(String importFilename, Function<String, String> convertLine) {
		try {
			relationChannel.truncate(0);
			numberBlocks = 0;
		}
		catch(IOException exception) {
			throw new RuntimeException("Error erasing database file " +relationName + ".db");
		}

		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(importFilename));

			String line = null;

			while((line = reader.readLine()) != null) {
				T tuple = tupleMaker.get();

				tuple.load(convertLine.apply(line));

				appendRecord(new Record<T>(tuple));
			}

			save();
		}
		catch(IOException exception) {
			throw new RuntimeException("Error accessing import file " + importFilename);
		}
		finally {
			try {
				if(reader != null) {
					reader.close();
				}
			}
			catch(IOException exception) {
				throw new RuntimeException("Error closing import file " + importFilename);
			}
		}
	}
}

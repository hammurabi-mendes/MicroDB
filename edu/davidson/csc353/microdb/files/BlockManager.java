/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.files;

import java.util.TreeMap;

import edu.davidson.csc353.microdb.utils.DecentPQ;

public class BlockManager<T extends Tuple> {
	private final int CAPACITY = 4;

	private Relation<T> relation;

	private TreeMap<Integer, BlockTimestamp> blockMap;
	private DecentPQ<BlockTimestamp> blockPQ;

	private class BlockTimestamp implements Comparable<BlockTimestamp> {
		public Block<T> block;
		public long lastUsed;

		public BlockTimestamp(Block<T> block, long lastUsed) {
			this.block = block;
			this.lastUsed = lastUsed;
		}

		public int compareTo(BlockTimestamp other) {
			return (int) (lastUsed - other.lastUsed);
		}
	}

	public BlockManager(Relation<T> relation) {
		this.relation = relation;

		this.blockMap = new TreeMap<>();
		this.blockPQ = new DecentPQ<>();
	}

	public boolean contains(int blockNumber) {
		return blockMap.containsKey(blockNumber);
	}

	public void put(int blockNumber, Block<T> block) {
		if(blockMap.size() >= CAPACITY) {
			evict();
		}

		BlockTimestamp newest = new BlockTimestamp(block, System.nanoTime());

		System.out.println("Inserted block # " + blockNumber);

		blockMap.put(blockNumber, newest);
		blockPQ.add(newest);
	}

	private void evict() {
		BlockTimestamp oldest = blockPQ.removeMin();
		blockMap.remove(oldest.block.getBlockNumber());

		relation.writeBlock(oldest.block);

		System.out.println("Evicted block # " + oldest.block.getBlockNumber());
	}

	public void delete(int blockNumber) {
		BlockTimestamp blockTimestamp = blockMap.get(blockNumber);

		if(blockTimestamp != null) {
			blockMap.remove(blockNumber);
			blockPQ.remove(blockTimestamp);
		}
	}

	public Block<T> get(int blockNumber) {
		System.out.println("Updated timestamp on block # " + blockNumber);

		BlockTimestamp blockTimestamp = blockMap.get(blockNumber);

		blockTimestamp.lastUsed = System.nanoTime();
		blockPQ.increaseKey(blockTimestamp);

		return blockTimestamp.block;
	}
}

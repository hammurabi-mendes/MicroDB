/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import java.util.function.Function;

import edu.davidson.csc353.microdb.files.*;

public class PrimaryIndexMaterialized<T extends Tuple, K extends Comparable<K>> implements PrimaryIndex<T, K> {
	private Queriable<T> queriable;
	private Function<T,K> keyExtractor;

	public PrimaryIndexMaterialized(Queriable<T> queriable, Function<T, K> keyExtractor) {
		this.queriable = queriable;
		this.keyExtractor = keyExtractor;
	}

	// O(log(n))
	public RecordLocation get(K key) {
		Block<T> foundBlock = binarySearch(key, 0, queriable.getNumberBlocks() - 1);

		Record<T> firstEqualBigger = firstEqualBigger(key, foundBlock);

		if(firstEqualBigger == null) {
			return new RecordLocation(queriable.getNumberBlocks(), 0);
		}

		return new RecordLocation(firstEqualBigger.getBlockNumber(), firstEqualBigger.getRecordNumber());
	}

	private Block<T> binarySearch(K key, int low, int high) {
		int middle = (low + high) / 2;

		Block<T> middleBlock = queriable.get(middle);

		int compareResult = compare(key, middleBlock);

		if(compareResult < 0) {
			if(middle - 1 < low) {
				return middleBlock;
			}

			return binarySearch(key, low, middle - 1);
		}
		else if(compareResult > 0) {
			if(middle + 1 > high) {
				return middleBlock;
			}

			return binarySearch(key, middle + 1, high);
		}

		Block<T> current = middleBlock;

		for(int i = middle; i >= 0; i--) {
			Block<T> previous = queriable.get(i);

			if(compare(key, previous) == 0) {
				current = previous;
			}
			else {
				break;
			}
		}

		return current;
	}

	private int compare(K key, Block<T> block) {
		int size = block.getRecords().size();

		Record<T> first = block.getRecord(0);
		Record<T> last = block.getRecord(size - 1);

		if(compare(key, first) < 0) {
			return -1;
		}

		if(compare(key, last) > 0) {
			return 1;
		}

		return 0;
	}	

	private int compare(K key, Record<T> record) {
		return key.compareTo(keyExtractor.apply(record.getTuple()));
	}	

	private Record<T> firstEqualBigger(K key, Block<T> block) {
		for(Record<T> record: block.getRecords()) {
			if(compare(key, record) <= 0) {
				return record;
			}
		}

		return null;
	}
}
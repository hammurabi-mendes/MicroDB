/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.indexes;

import java.util.Iterator;
import java.util.function.Function;

import edu.davidson.csc353.microdb.files.*;
import edu.davidson.csc353.microdb.operations.*;
import edu.davidson.csc353.microdb.sorting.*;

import edu.davidson.csc353.microdb.operations.ConversionIterator;
import edu.davidson.csc353.microdb.operations.IterableWrapper;

public class SecondaryIndexMaterialized<T extends Tuple, K extends Comparable<K>> implements SecondaryIndex<T, K> {
	private Function<K, String> saveKey;

	private Queriable<IndexEntry> sortedIndexEntries;
	private PrimaryIndexMaterialized<IndexEntry, String> indexSortedIndexEntries;

	public SecondaryIndexMaterialized(Queriable<T> queriable, Function<T, K> keyExtractor, Function<K, String> saveKey, Function<String, K> loadKey) {
		this.saveKey = saveKey;

		Relation<IndexEntry> indexEntries = new Relation<IndexEntry>("sec_index_entries", () -> new IndexEntry(null, 0, 0));
		indexEntries.clear();

		for(Record<T> record: queriable) {
			K key = keyExtractor.apply(record.getTuple());

			indexEntries.appendRecord(new Record<IndexEntry>(new IndexEntry(saveKey.apply(key), record.getBlockNumber(), record.getRecordNumber())));
		}

		Sorter<IndexEntry, String> indexEntriesSorter = new Sorter<>(indexEntries, () -> new IndexEntry(null, 0, 0), t -> t.key);
		indexEntriesSorter.setEliminateDuplicates(false);

		sortedIndexEntries = indexEntriesSorter.sort();

		indexSortedIndexEntries = new PrimaryIndexMaterialized<>(sortedIndexEntries, t -> t.key);
	}

	// O(log(n))
	public Iterable<RecordLocation> get(K key) {
		Selector<IndexEntry> matchingIndexEntriesSelector = new Selector<>(sortedIndexEntries);

		Queriable<IndexEntry> matchingIndexEntries = matchingIndexEntriesSelector.selectMatchingKeys(indexSortedIndexEntries, saveKey.apply(key), t -> t.key);

		Iterator<RecordLocation> matchingRecordsIterator = new ConversionIterator<Record<IndexEntry>, RecordLocation>(matchingIndexEntries.iterator(), e -> {
			IndexEntry entry = e.getTuple();

			return new RecordLocation(entry.blockNumber, entry.recordNumber);
		});

		return new IterableWrapper<RecordLocation>(matchingRecordsIterator);
	}
}
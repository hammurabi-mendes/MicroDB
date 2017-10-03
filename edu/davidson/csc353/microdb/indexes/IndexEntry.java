package edu.davidson.csc353.microdb.indexes;

import edu.davidson.csc353.microdb.files.Tuple;

public class IndexEntry implements Tuple {
	public String key;
	public int blockNumber;
	public int recordNumber;

	public IndexEntry(String key, int blockNumber, int recordNumber) {
		this.key = key;
		this.blockNumber = blockNumber;
		this.recordNumber = recordNumber;
	}

	public void load(String input) {
		String[] fields = input.split(",");

		key = fields[0];
		blockNumber = Integer.parseInt(fields[1]);
		recordNumber = Integer.parseInt(fields[2]);
	}

	public String save() {
		return key + "," + blockNumber + "," + recordNumber;
	}

	public int getSize() {
		return save().getBytes().length;
	}

	public String toString() {
		return "[" + save() + "]";
	}
}

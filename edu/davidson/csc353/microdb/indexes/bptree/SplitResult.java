package edu.davidson.csc353.microdb.indexes.bptree;

/**
 * Class that represents the result of splitting a node.
 * 
 * @param <K> Type of the keys in the B+Tree node.
 * @param <V> type of the values associated with the keys in the B+Tree node.
 */
public class SplitResult<K extends Comparable<K>, V> {
	/**
	 * Reference to the left block in the split operation.
	 */
	public BPNode<K,V> left;

	/**
	 * Reference to the right block in the split operation.
	 */
	public BPNode<K,V> right;
	
	/**
	 * When splitting leaf nodes:
	 * 	- the first key in the right B+Tree node
	 * 
	 * When splitting internal nodes:
	 * 	- the divider key that will be subsequently be inserted in the parent node
	 */
	public K dividerKey;
	
	/**
	 * Returns a string representation of the split result.
	 */
	public String toString() {
		return "<" + left + ", " + dividerKey + ", " + right + ">";
	}
}

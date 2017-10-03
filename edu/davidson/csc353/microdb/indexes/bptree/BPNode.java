package edu.davidson.csc353.microdb.indexes.bptree;

import java.nio.ByteBuffer;

import java.util.ArrayList;

import java.util.function.Function;

/**
 * B+Tree node (internal or leaf).
 * 
 * @param <K> Type of the keys in the B+Tree node.
 * @param <V> type of the values associated with the keys in the B+Tree node.
 */
public class BPNode<K extends Comparable<K>, V> {
	// Maximum number of children in an internal node (fanout)
	// A leaf node can have at most SIZE - 1 key-value pairs
	public static int SIZE = 5;

	// Flag indicating if a leaf or not
	boolean leaf;

	// Reference to the parent
	public int parent;

	// Keys:
	// -  in the leaf node, they are the associated with values
	// -  in internal nodes, they separate the pointers to the children
	public ArrayList<K> keys;

	// (Leaf node only) Values associated with each key
	public ArrayList<V> values;
	// (Leaf node only) Next leaf node
	public int next;

	// (Internal node only) Children of the internal node
	public ArrayList<Integer> children; 

	// Node number
	public int number;

	/**
	 * Constructor.
	 * 
	 * @param leaf Flag indicating whether the node is a leaf (true) or internal node (false)
	 */
	public BPNode(boolean leaf) {
		this.leaf = leaf;

		this.parent = -1;
		this.next = -1;

		// Contains one more than you allow, because you may want to add, and then split
		this.keys = new ArrayList<>(SIZE);

		// Contains one more than you allow, because you may want to add, and then split
		this.values = new ArrayList<>(SIZE);

		// Contains one more than you allow, because you may want to add, and then split
		this.children = new ArrayList<>(SIZE + 1);
	}

	/**
	 * Returns true if the B+Tree node is a leaf node.
	 * 
	 * @return True if the B+Tree node is a leaf node.
	 */
	public boolean isLeaf() {
		return leaf;
	}

	/**
	 * Returns the key in a given position.
	 * 
	 * @param index Position of the key.
	 * 
	 * @return Key in the specified position.
	 */
	public K getKey(int index) {
		return keys.get(index);
	}

	/**
	 * Returns the child in a given position.
	 * This can only be called for internal nodes.
	 * 
	 * @param index Position of the child.
	 * 
	 * @return Child in the specified position.
	 */
	public int getChild(int index) {
		return children.get(index);
	}

	/**
	 * Returns the value in a given position.
	 * This can only be called for leaf nodes.
	 * 
	 * @param index Position of the value.
	 * 
	 * @return Value in the specified position.
	 */
	public V getValue(int index) {
		return values.get(index);
	}

	/**
	 * (Leaf node only) Inserts a (K,P) pair in the appropriate position in the node.
	 * 
	 * @param key The key.
	 * @param value The value associated with the key.
	 */
	public void insertValue(K key, V value) {
		int i;

		for(i = 0; i < keys.size(); i++) {
			if(BPTree.more(keys.get(i), key)) {
				break;
			}
		}

		keys.add(i, key);
		values.add(i, value);
	}

	/** 
	 * (Internal node only) Inserts a (K,P) pair in the appropriate position in the node.
	 * 
	 * NOTE: The pointer associated with key at position i is actually located at position i+1
	 *       due to the first pointer.
	 * 
	 * @param key The key.
	 * @param value The child node reference that should FOLLOW the key.
	 * @param nodeFactory Factory that generates new nodes.
	 */
	public void insertChild(K key, int childNumber, BPNodeFactory<K,V> nodeFactory) {
		int i;

		for(i = 0; i < keys.size(); i++) {
			if(BPTree.more(keys.get(i), key)) {
				break;
			}
		}

		keys.add(i, key);
		children.add(i + 1, childNumber);

		BPNode<K,V> child = nodeFactory.getNode(childNumber);
		
		child.parent = this.number;
		//nodeFactory.save(child);
	}

	/**
	 * Splits an overflowed leaf node into a {@link SplitResult} object,
	 * that contains:
	 * 	- A reference to the left B+Tree node;
	 * 	- A reference to the right B+Tree node;
	 * 	- The first key in the right B+Tree node (the "divider Key" in the {@link SplitResult} object).
	 * 
	 * The method also makes the left node point to the right node in the "next" attribute.
	 * 
	 * @param nodeFactory Factory that generates new nodes.
	 * 
	 * @return A new {@link SplitResult} following the specifications above.
	 */
	public SplitResult<K,V> splitLeaf(BPNodeFactory<K,V> nodeFactory) {
		SplitResult<K,V> result = new SplitResult<>();

		result.left = this;
		result.right = nodeFactory.create(true);

		result.right.parent = this.parent;

		result.left.next = result.right.number;

		int removeCount = 0;

		for(int i = keys.size() / 2; i < keys.size(); i++) {
			result.right.keys.add(keys.get(i));
			result.right.values.add(values.get(i));

			removeCount++;
		}

		for(int i = 0; i < removeCount; i++) {
			result.left.keys.remove(result.left.keys.size() - 1);
			result.left.values.remove(result.left.values.size() - 1);
		}

		result.dividerKey = result.right.getKey(0);
		
		//nodeFactory.save(result.left);
		//nodeFactory.save(result.right);

		return result;
	}

	/**
	 * Splits an overflowed leaf internal into a {@link SplitResult} object,
	 * that contains:
	 * 	- A reference to the left B+Tree node;
	 * 	- A reference to the right B+Tree node;
	 * 	- The divider key that will be subsequently be inserted in the parent node
	 *    (stored in the "divider Key" in the {@link SplitResult} object).
	 *    
	 * @param nodeFactory Factory that generates new nodes.
	 * 
	 * @return A new {@link SplitResult} following the specifications above.
	 */
	public SplitResult<K, V> splitInternal(BPNodeFactory<K,V> nodeFactory) {
		SplitResult<K,V> result = new SplitResult<>();

		result.left = this;
		result.right = nodeFactory.create(false);

		result.right.parent = this.parent;

		int removeCount = 0;

		for(int i = (keys.size() / 2 + 1); i < keys.size(); i++) {
			result.right.keys.add(keys.get(i));
			result.right.children.add(children.get(i + 1));

			removeCount++;
		}

		for(int i = 0; i < removeCount; i++) {
			result.left.keys.remove(result.left.keys.size() - 1);
			result.left.children.remove(result.left.children.size() - 1);
		}

		result.dividerKey = result.left.keys.remove(result.left.keys.size() - 1);
		result.right.children.add(0, result.left.children.remove(result.left.children.size() - 1));

		//nodeFactory.save(result.left);
		//nodeFactory.save(result.right);

		for(int i = 0; i < result.right.children.size(); i++) {
			BPNode<K,V> child = nodeFactory.getNode(result.right.children.get(i));

			child.parent = result.right.number;
			//nodeFactory.save(child);
		}

		return result;
	}

	/**
	 * Returns a string representation of the B+Tree node.
	 */
	public String toString() {
		String result = "[(#: " + number + ") ";

		// If leaf, print [k1 v1 k2 v2 ... kn vn]
		if(isLeaf()) {
			for(int i = 0; i < keys.size() - 1; i++) {
				result += keys.get(i);
				result += " ";
				result += values.get(i);
				result += " ";
			}

			if(keys.size() > 0) {
				result += keys.get(keys.size() - 1);
				result += " ";
				result += values.get(keys.size() - 1);
			}
		}
		// If internal, print [<c0> k1 <c1> k2 <c2> ... kn <cn>], where
		else {
			for(int i = 0; i < keys.size(); i++) {
				result += "<" + children.get(i) + ">";
				result += " ";
				result += keys.get(i);
				result += " ";
			}

			if(keys.size() > 0) {
				result += "<" + children.get(keys.size()) + ">";
			}
		}

		result += "]";

		return result;
	}

	/**
	 * Loads information from a buffer of 512 bytes (loaded from disk)
	 * into the memory representation of a B+Tree node.
	 * 
	 * @param buffer A byte buffer of 512 bytes containing the node representation from disk.
	 * @param loadKey A function that converts string representations into keys of type K
	 * @param loadValue A function that converts string representations into values of type V
	 */
	public void load(ByteBuffer buffer, Function<String, K> loadKey, Function<String, V> loadValue) {
		buffer.rewind();

		leaf = (buffer.getInt() == 1 ? true : false);

		byte[] data = new byte[buffer.getInt()];

		buffer.get(data);

		String representation = new String(data);

		if(representation.length() > 0) {
			if(leaf) {
				String[] fields = representation.split("#");

				for(int i = 0; i < fields.length / 2; i++) {
					keys.add(loadKey.apply(fields[i]));
				}

				for(int i = fields.length / 2; i < fields.length; i++) {
					values.add(loadValue.apply(fields[i]));
				}
			}
			else {
				String[] fields = representation.split("#");

				for(int i = 0; i < fields.length / 2; i++) {
					keys.add(loadKey.apply(fields[i]));
				}

				for(int i = fields.length / 2; i < fields.length; i++) {
					children.add(Integer.parseInt(fields[i]));
				}
			}
		}

		parent = buffer.getInt();
		next = buffer.getInt();

		number = buffer.getInt();
	}

	/**
	 * Save the memory representation of the B+Tree node
	 * into a buffer of 512 bytes (to be stored on disk).
	 * 
	 * @param buffer
	 */
	public void save(ByteBuffer buffer) {
		buffer.rewind();

		String representation = "";

		if(isLeaf()) {
			buffer.putInt(1);

			for(int i = 0; i < keys.size(); i++) {
				representation += keys.get(i).toString();
				representation += "#";
			}

			for(int i = 0; i < values.size(); i++) {
				representation += values.get(i).toString();
				representation += "#";
			}

			if(representation.length() > 0) {
				representation = representation.substring(0, representation.length() - 1); //ugh
			}
		}
		else {
			buffer.putInt(0);

			for(int i = 0; i < keys.size(); i++) {
				representation += keys.get(i).toString();
				representation += "#";
			}

			for(int i = 0; i < children.size(); i++) {
				representation += children.get(i);
				representation += "#";
			}

			if(representation.length() > 0) {
				representation = representation.substring(0, representation.length() - 1); //ugh
			}
		}

		byte[] data = representation.getBytes();

		buffer.putInt(data.length);
		buffer.put(data);

		buffer.putInt(parent);
		buffer.putInt(next);

		buffer.putInt(number);
	}
}

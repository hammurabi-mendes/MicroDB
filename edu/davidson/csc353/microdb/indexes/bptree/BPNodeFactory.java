package edu.davidson.csc353.microdb.indexes.bptree;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.io.RandomAccessFile;

import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.function.Function;

import edu.davidson.csc353.microdb.utils.DecentPQ;

public class BPNodeFactory<K extends Comparable<K>, V> {
	public static final int DISK_SIZE = 512;

	public static final int CAPACITY = 10;

	private String indexName;

	private Function<String, K> loadKey;
	private Function<String, V> loadValue;

	private int numberNodes;

	private RandomAccessFile relationFile;
	private FileChannel relationChannel;

	private HashMap<Integer, NodeTimestamp> nodeMap;
	private DecentPQ<NodeTimestamp> nodePQ;

	private class NodeTimestamp implements Comparable<NodeTimestamp> {
		public BPNode<K,V> node;
		public long lastUsed;

		public NodeTimestamp(BPNode<K,V> node, long lastUsed) {
			this.node = node;
			this.lastUsed = lastUsed;
		}

		public int compareTo(NodeTimestamp other) {
			return (int) (lastUsed - other.lastUsed);
		}
	}

	/**
	 * Creates a new NodeFactory object, which will operate a buffer manager for
	 * the nodes of a B+Tree.
	 * 
	 * @param indexName The name of the index stored on disk.
	 */
	public BPNodeFactory(String indexName, Function<String, K> loadKey, Function<String, V> loadValue) {
		try {
			this.indexName = indexName;
			this.loadKey = loadKey;
			this.loadValue = loadValue;

			Files.delete(Paths.get(indexName + ".db"));

			relationFile = new RandomAccessFile(indexName + ".db", "rws");
			relationChannel = relationFile.getChannel();

			numberNodes = (int) (relationChannel.size() / DISK_SIZE);

			nodeMap = new HashMap<>();
			nodePQ = new DecentPQ<>();
		}
		catch (FileNotFoundException exception) {
			// Ignore: a new file has been created
		}
		catch(IOException exception) {
			throw new RuntimeException("Error accessing " + indexName);
		}
	}

	/**
	 * Creates a B+Tree node.
	 * 
	 * @param leaf Flag indicating whether the node is a leaf (true) or internal node (false)
	 * 
	 * @return A new B+Tree node.
	 */
	public BPNode<K,V> create(boolean leaf) {
		if(nodeMap.size() >= CAPACITY) {
			evict();
		}

		BPNode<K,V> created = new BPNode<K,V>(leaf);
		created.number = numberNodes;

		setupNode(created);

		//System.out.println("Created node: " + created + ", parent = " + created.parent + ", next = " + created.next + ", number = " + created.number);

		numberNodes++;

		return created;
	}

	/**
	 * Inserts a node into the buffer manager's map and priority queue.
	 * 
	 * @param node Node to be inserted into the buffer manager's map and priority queue.
	 */
	private void setupNode(BPNode<K,V> node) {
		NodeTimestamp nodeTimestamp = new NodeTimestamp(node, System.nanoTime());

		nodeMap.put(node.number, nodeTimestamp);
		nodePQ.add(nodeTimestamp);	
	}

	/**
	 * Saves a node into disk.
	 * 
	 * @param node Node to be saved into disk.
	 */
	public void save(BPNode<K,V> node) {
		writeNode(node);
	}

	/**
	 * Reads a node from the disk.
	 * 
	 * @param nodeNumber Number of the node read.
	 * 
	 * @return Node read from the disk that has the provided number.
	 */
	private BPNode<K,V> readNode(int nodeNumber) {
		ByteBuffer nodeBuffer = ByteBuffer.allocate(DISK_SIZE);

		BPNode<K,V> node = new BPNode<K,V>(false);

		try {
			relationChannel.read(nodeBuffer, nodeNumber * DISK_SIZE);

			node.load(nodeBuffer, loadKey, loadValue);
		}
		catch (IOException e) {
			throw new RuntimeException("Error accessing " + nodeNumber + " on file " + indexName);
		}

		return node;		
	}

	/**
	 * Writes a node into disk.
	 * 
	 * @param node Node to be saved into disk.
	 */
	private void writeNode(BPNode<K,V> node) {
		ByteBuffer nodeBuffer = ByteBuffer.allocate(DISK_SIZE);

		node.save(nodeBuffer);

		int nodeNumber = node.number;

		try {
			nodeBuffer.rewind();
			relationChannel.write(nodeBuffer, nodeNumber * DISK_SIZE);
		}
		catch (IOException e) {
			throw new RuntimeException("Error writing " + nodeNumber + " on index " + indexName);
		}
	}

	/**
	 * Evicts the last recently used node back into disk.
	 */
	private void evict() {
		NodeTimestamp oldest = nodePQ.removeMin();

		BPNode<K,V> node = oldest.node;
		int nodeNumber = node.number;

		nodeMap.remove(nodeNumber);

		writeNode(node);

		//System.out.println("Evicted node # " + nodeNumber);
	}

	/**
	 * Returns the node associated with a particular number.
	 * 
	 * @param number The number to be converted to node (loading it from disk, if necessary).
	 * 
	 * @return The node associated with the provided number.
	 */
	public BPNode<K,V> getNode(int number) {
		if(!nodeMap.containsKey(number)) {
			if(nodeMap.size() >= CAPACITY) {
				evict();
			}

			BPNode<K,V> loaded = readNode(number);

			setupNode(loaded);

			//System.out.println("Loaded node: " + loaded + ", parent = " + loaded.parent + ", next = " + loaded.next + ", number = " + loaded.number);
			return loaded;
		}

		NodeTimestamp nodeTimestamp = nodeMap.get(number);
		nodeTimestamp.lastUsed = System.nanoTime();

		nodePQ.increaseKey(nodeTimestamp);
		return nodeTimestamp.node;
	}
}

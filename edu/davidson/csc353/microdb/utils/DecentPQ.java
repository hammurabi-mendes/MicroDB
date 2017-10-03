/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.utils;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.Collections;

/**
 * Decent heap for Java users.
 */
public class DecentPQ<T extends Comparable<T>> {
	private ArrayList<T> nodes;
	private HashMap<T, Integer> positionMap;

	/**
	 * Initializes a new MyHeap.
	 */
	public DecentPQ() {
		nodes = new ArrayList<>();
		positionMap = new HashMap<>();
	}
	
	/**
	 * Returns the size of the min-heap.
	 * 
	 * @return The size of the min-heap.
	 */
	public int size() {
		return nodes.size();
	}
	
	/**
	 * Swaps positions x and y in the nodes array.
	 * 
	 * @param x First position.
	 * @param y Second position.
	 */
	private void swap(int x, int y) {
		positionMap.put(nodes.get(x), y);
		positionMap.put(nodes.get(y), x);

		Collections.swap(nodes, x, y);
	}
	
	/**
	 * Given a position i, return the position of the parent node.
	 * 
	 * @param i Position to check for the parent node.
	 * 
	 * @return Position of the parent node.
	 */
	private int parent(int i) {
		return (i-1)/2;
	}
	
	/**
	 * Given a position i, return the position of the left child.
	 * 
	 * @param i Position to check for the left child.
	 * 
	 * @return Position of the left child.
	 */
	private int leftChild(int i) {
		return 2*i+1;
	}
	
	/**
	 * Given a position i, return the position of the right child.
	 * 
	 * @param i Position to check for the right child.
	 * 
	 * @return Position of the right child.
	 */
	private int rightChild(int i) {
		return 2*i+2;
	}
	
	/**
	 * Add value to the min-heap.
	 * 
	 * @param element Value to be added to the min-heap.
	 */
	public void add(T element) {
		// Add to the end of the array
		nodes.add(element);
		positionMap.put(element, nodes.size() - 1);
		
		// Set current position to the newly added element
		int i = nodes.size() - 1;

		// While you are not the root element,
		// and the parent is bigger, swap with the parent
		// then update your current position
		moveUp(i);
	}
	
	/**
	 * Remove the smallest value from the min-heap.
	 * 
	 * @return The former smallest value from the min-heap.
	 */
	public T removeMin() {
		return remove(0);
	}
	
	public T remove(T element) {
		return remove(positionMap.get(element));
	}
	
	public T remove(int position) {
		if(size() == 0) {
			return null;
		}
		
		// Get the removed element
		T removed = nodes.get(position);
		positionMap.remove(removed);

		// Remove from the end
		T last = nodes.remove(nodes.size() - 1);
		
		// If we removed the last element, we're done
		if(removed == last) {
			return last;
		}

		// Otherwise, replace the last element into the remove position and move down
		nodes.set(position, last);
		positionMap.put(last, position);
		
		moveDown(position);
		
		return removed;	
	}
	
	public void decreaseKey(T element) {
		moveUp(positionMap.get(element));
	}

	public void increaseKey(T element) {
		moveDown(positionMap.get(element));
	}
	
	private int moveUp(int position) {
		int i = position;
		
		// While you are not the root element,
		// and the parent is bigger, swap with the parent
		// then update your current position
		while(i > 0 && nodes.get(parent(i)).compareTo(nodes.get(i)) > 0) {
			swap(i, parent(i));
			i = parent(i);
		}
		
		return i;
	}
	
	private int moveDown(int position) {
		// Set current position to the root of the heap
		int i = position;
		
		// Switch spots with your smallest child as long as the
		// child's position does not go past the end of the heap
		while(true) {
			int swap = i;

			// Find the smallest child that is valid (i.e., smaller than nodes.size())
			if(leftChild(i) < nodes.size() && nodes.get(leftChild(i)).compareTo(nodes.get(swap)) < 0) {
				swap = leftChild(i);
			}
			
			if(rightChild(i) < nodes.size() && nodes.get(rightChild(i)).compareTo(nodes.get(swap)) < 0) {
				swap = rightChild(i);
			}
			
			// If you can't find such child, or the smallest valid child is already in place,
			// then you're done: break the loop
			if(swap == i) {
				break;
			}
			else {
				swap(swap, i);
				i = swap;
			}
		}
		
		return i;
	}
	
	public T peek() {
		if(nodes.size() > 0) {
			return nodes.get(0);
		}
		
		return null;
	}
}


/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.college;

import edu.davidson.csc353.microdb.files.Tuple;

public class Classroom implements Tuple {
	public String building;
	public String roomNumber;
	public short capacity;

	public void load(String input) {
		String[] fields = input.split(",");

		building = fields[0];
		roomNumber = fields[1];
		capacity = Short.parseShort(fields[2]);
	}

	public String save() {
		return building + "," + roomNumber + "," + capacity;
	}

	public int getSize() {
		return save().getBytes().length;
	}
	
	public String toString() {
		return "[" + save() + "]";
	}
}

/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.college;

import edu.davidson.csc353.microdb.files.Tuple;

public class Department implements Tuple {
	public String name;
	public String building;
	public double budget;

	public void load(String input) {
		String[] fields = input.split(",");

		name = fields[0];
		building = fields[1];
		budget = Double.parseDouble(fields[2]);
	}

	public String save() {
		return name + "," + building + "," + budget;
	}

	public int getSize() {
		return save().getBytes().length;
	}
	
	public String toString() {
		return "[" + save() + "]";
	}
}

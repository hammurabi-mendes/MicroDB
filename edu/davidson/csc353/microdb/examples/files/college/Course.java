/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.college;

import edu.davidson.csc353.microdb.files.Tuple;

public class Course implements Tuple {
	public String id;
	public String title;
	public String department;
	public int credits;

	public void load(String input) {
		String[] fields = input.split(",");

		id = fields[0];
		title = fields[1];
		department = fields[2];
		credits = Integer.parseInt(fields[3]);
	}

	public String save() {
		return id + "," + title + "," + department + "," + credits;
	}

	public int getSize() {
		return save().getBytes().length;
	}
	
	public String toString() {
		return "[" + save() + "]";
	}
}

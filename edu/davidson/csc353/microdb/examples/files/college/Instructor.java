/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.college;

import edu.davidson.csc353.microdb.files.Tuple;

public class Instructor implements Tuple {
	public int id;
	public String name;
	public String department;
	public double salary;

	public void load(String input) {
		String[] fields = input.split(",");

		id = Integer.parseInt(fields[0]);
		name = fields[1];
		department = fields[2];
		salary = Double.parseDouble(fields[3]);
	}

	public String save() {
		return id + "," + name + "," + department + "," + salary;
	}

	public int getSize() {
		return save().getBytes().length;
	}
	
	public String toString() {
		return "[" + save() + "]";
	}
}

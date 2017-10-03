/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.simple;

import edu.davidson.csc353.microdb.files.Tuple;

public class Employee implements Tuple {
	public String name;
	public Double salary;

	public void load(String input) {
		String[] fields = input.split("\\$");

		name = fields[0];
		salary = Double.parseDouble(fields[1]);
	}

	public String save() {
		return name + "$" + salary;
	}

	public int getSize() {
		return save().getBytes().length;
	}
	
	public String toString() {
		return "[" + name + ", " + salary + "]";
	}
}
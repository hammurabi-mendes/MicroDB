/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.simple;

import edu.davidson.csc353.microdb.files.Record;

import edu.davidson.csc353.microdb.files.Relation;

public class Test {
	public static void main(String[] args) {
		loadCSV("employee");
		loadDB("employee");
	}

	private static void loadCSV(String relationName) {
		Relation<Employee> employee = new Relation<>(relationName, () -> new Employee());

		employee.importFromFile(relationName + ".csv", (line) -> {
			return line.replace(',', '$');
		});

		for(Record<Employee> record: employee) {
			System.out.println(record);
		}	
	}
	
	private static void loadDB(String relationName) {
		Relation<Employee> employee1 = new Relation<>(relationName, () -> new Employee());

		for(Record<Employee> record: employee1) {
			System.out.println(record);
		}	
	}
}

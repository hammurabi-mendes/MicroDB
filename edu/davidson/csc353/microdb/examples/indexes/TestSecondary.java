/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.indexes;

import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Block;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.indexes.*;

import edu.davidson.csc353.microdb.examples.files.college.*;

import edu.davidson.csc353.microdb.files.Queriable;

public class TestSecondary {
	public static void main(String[] args) {
		Block.SIZE = 512;

		Queriable<Student> student = new Relation<>("student", () -> new Student());

		// Prints all students
		for(Record<Student> record: student) {
			System.out.println(record);
		}

		System.out.println("---------------");

		MemorySecondaryIndex<Student, String> studentIndex = new DenseSecondaryIndexTree<Student, String>(student, (tuple) -> tuple.department);

		for(RecordLocation location: studentIndex.get("Astrophysiology")) { // Prints "[null]"
			System.out.println(location);
		}

		System.out.println("---------------");

		for(RecordLocation location: studentIndex.get("Comp. Sci.")) { // Prints "[(1, 10) ... (114, 12)]"
			System.out.println(location);
		}

		System.out.println("---------------");

		for(String key: studentIndex.allKeys()) {
			System.out.println(key);
		}

		System.out.println("---------------");

		for(String key: studentIndex.allKeys("Pol. Sci.")) {
			System.out.println(key);
		}
	}
}

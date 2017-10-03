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

public class TestPrimary {
	public static void main(String[] args) {
		Block.SIZE = 512;

		Queriable<Student> student = new Relation<>("student", () -> new Student());
		
		// Prints all students
		for(Record<Student> record: student) {
			System.out.println(record);
		}
		
		MemoryPrimaryIndex<Student, String> studentIndex = new DensePrimaryIndexArray<Student, String>(student, (tuple) -> tuple.name);

		RecordLocation studentPosition = null;
		
		studentPosition = studentIndex.get("Hermione"); // Prints "null"
		System.out.println(studentPosition);

		studentPosition = studentIndex.get("Saad"); // Prints "(100, 4)"
		System.out.println(studentPosition);
		
		System.out.println("---------------");

		for(String key: studentIndex.allKeys()) {
			System.out.println(key);
		}
		
		System.out.println("---------------");

		for(String key: studentIndex.allKeys("Zuo")) {
			System.out.println(key);
		}
	}
}

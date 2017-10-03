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
import edu.davidson.csc353.microdb.sorting.Sorter;
import edu.davidson.csc353.microdb.examples.files.college.*;

import edu.davidson.csc353.microdb.files.Queriable;

public class TestPrimaryMaterialized {
	public static void main(String[] args) {
		Block.SIZE = 512;

		Queriable<Student> students = new Relation<>("student", () -> new Student());

		Sorter<Student, String> sorter = new Sorter<>(students, () -> new Student(), t -> t.name);

		// Change from true to false to check
		sorter.setEliminateDuplicates(true);
		Queriable<Student> sortedStudents = sorter.sort();

		// Prints all students
		for(Record<Student> record: sortedStudents) {
			System.out.println(record);
		}
		
		PrimaryIndex<Student, String> studentIndex = new PrimaryIndexMaterialized<Student, String>(sortedStudents, (tuple) -> tuple.name);

		RecordLocation studentPosition = null;
		
		studentPosition = studentIndex.get("Hermione"); // Prints "(37, 13) [duplicates] (32, 3) [non-duplicates]"
		System.out.println(studentPosition);

		studentPosition = studentIndex.get("Saad"); // Prints "(80, 1) [duplicates] (69, 1) [non-duplicates]"
		System.out.println(studentPosition);

		studentPosition = studentIndex.get("Rieger"); // Prints "(77, 15) [duplicates] (67, 2) [non-duplicates]"
		System.out.println(studentPosition);
	}
}

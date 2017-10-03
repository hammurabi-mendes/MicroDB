/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.operations;

import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Block;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.files.*;
import edu.davidson.csc353.microdb.indexes.*;
import edu.davidson.csc353.microdb.operations.*;
import edu.davidson.csc353.microdb.examples.files.college.*;

public class TestJoin {
	public static void main(String[] args) {
		Block.SIZE = 512;

		testLoopJoin();

		System.out.println("---------------");

		testMergeJoin();
	}

	private static void testLoopJoin() {
		Queriable<Student> student = new Relation<>("student", () -> new Student());
		Queriable<Department> department = new Relation<>("department", () -> new Department());

		Joiner<Student, Department, StudentDepartment> joiner = new Joiner<>(student, department, (s, d) -> new StudentDepartment(s, d));
		Queriable<StudentDepartment> result = joiner.loopJoin((s, d) -> (s.department.equals(d.name)));

		for(Record<StudentDepartment> record: result) {
			System.out.println(record.getTuple());
		}
	}

	private static void testMergeJoin() {
		Queriable<Student> student = new Relation<>("student", () -> new Student());
		Queriable<Department> department = new Relation<>("department", () -> new Department());

		MemorySecondaryIndex<Student, String> studentIndex = new DenseSecondaryIndexTree<Student, String>(student, (tuple) -> tuple.department);
		MemorySecondaryIndex<Department, String> departmentIndex = new DenseSecondaryIndexTree<Department, String>(department, (tuple) -> tuple.name);

		Joiner<Student, Department, StudentDepartment> joiner = new Joiner<>(student, department, (s, d) -> new StudentDepartment(s, d));
		Queriable<StudentDepartment> result = joiner.mergeJoin(studentIndex, departmentIndex);

		for(Record<StudentDepartment> record: result) {
			System.out.println(record.getTuple());
		}
	}
}
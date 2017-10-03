/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.sorting;

import edu.davidson.csc353.microdb.examples.files.college.Student;

import edu.davidson.csc353.microdb.examples.files.simple.Employee;

import edu.davidson.csc353.microdb.files.Block;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.sorting.Sorter;

import edu.davidson.csc353.microdb.files.Queriable;

public class TestSort {
	public static void main(String[] args) {

		//testSmallSorting();
		testLargeSorting();
	}

	private static void testSmallSorting() {
		Queriable<Employee> employee = new Relation<>("employee", () -> new Employee());

		// Prints all students
		for(Record<Employee> record: employee) {
			System.out.println(record);
		}

		Sorter<Employee, Double> sorter = new Sorter<>(employee, () -> new Employee(), t -> t.salary);

		// Prints all students
		for(Record<Employee> record: sorter.sort()) {
			System.out.println(record);
		}
	}

	private static void testLargeSorting() {
		Block.SIZE = 512;

		Queriable<Student> student = new Relation<>("student", () -> new Student());

		// Prints all students
		for(Record<Student> record: student) {
			System.out.println(record);
		}

		Sorter<Student, String> sorter = new Sorter<>(student, () -> new Student(), t -> t.name);
		// Change from true to false to check
		sorter.setEliminateDuplicates(false);

		// Prints all students
		for(Record<Student> record: sorter.sort()) {
			System.out.println(record);
		}
	}
}

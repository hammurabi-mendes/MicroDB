/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.college;

import java.util.function.Supplier;
import java.util.function.Function;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Block;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.files.Queriable;

public class Test {
	public static void main(String[] args) {
		Block.SIZE = 512;

		loadCSVs();
		loadRelations();
	}
	
	private static void loadCSVs() {
		Relation<Student> student = loadCSV("student", () -> new Student());
		
		for(Record<Student> record: student) {
			System.out.println(record);
		}
		
		Relation<Instructor> instructor = loadCSV("instructor", () -> new Instructor());
		
		for(Record<Instructor> record: instructor) {
			System.out.println(record);
		}
		
		Relation<Course> course = loadCSV("course", () -> new Course());
		
		for(Record<Course> record: course) {
			System.out.println(record);
		}
		
		Relation<Department> department = loadCSV("department", () -> new Department());
		
		for(Record<Department> record: department) {
			System.out.println(record);
		}
		
		Relation<Classroom> classroom = loadCSV("classroom", () -> new Classroom());
		
		for(Record<Classroom> record: classroom) {
			System.out.println(record);
		}
	}

	private static void loadRelations() {
		Queriable<Student> student = new Relation<>("student", () -> new Student());
		
		for(Record<Student> record: student) {
			System.out.println(record);
		}
		
		Queriable<Instructor> instructor = new Relation<>("instructor", () -> new Instructor());
		
		for(Record<Instructor> record: instructor) {
			System.out.println(record);
		}
		
		Queriable<Course> course = new Relation<>("course", () -> new Course());
		
		for(Record<Course> record: course) {
			System.out.println(record);
		}
		
		Queriable<Department> department = new Relation<>("department", () -> new Department());
		
		for(Record<Department> record: department) {
			System.out.println(record);
		}
		
		Queriable<Classroom> classroom = new Relation<>("classroom", () -> new Classroom());
		
		for(Record<Classroom> record: classroom) {
			System.out.println(record);
		}
	}

	private static String convertLine(String line) {
		String replaced = line;
		
		replaced = replaced.replaceAll("'", "");
		replaced = replaced.replaceAll(", ", ",");
		
		return replaced;
	}

	private static <T extends Tuple> Relation<T> loadCSV(String relationName, Supplier<T> tupleMaker) {
		return loadCSV(relationName, tupleMaker, Test::convertLine);
	}

	private static <T extends Tuple> Relation<T> loadCSV(String relationName, Supplier<T> tupleMaker, Function<String, String> convertLine) {
		Relation<T> relation = new Relation<T>(relationName, tupleMaker);

		relation.importFromFile(relationName + ".csv", convertLine);
		
		return relation;
	}
}

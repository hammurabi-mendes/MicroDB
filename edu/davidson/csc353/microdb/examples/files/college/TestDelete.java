/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.files.college;

import edu.davidson.csc353.microdb.files.Record;

import java.util.function.Function;
import java.util.function.Supplier;

import edu.davidson.csc353.microdb.files.Block;
import edu.davidson.csc353.microdb.files.Relation;
import edu.davidson.csc353.microdb.files.Tuple;

public class TestDelete {
	public static void main(String[] args) {
		Block.SIZE = 512;

		Relation<Student> student = loadCSV("student", () -> new Student());
		
		for(Record<Student> record: student) {
			System.out.println(record);
		}
		
		// Remove from "McCarter" to "Cochran". Should display: "Namer" to "Yalk" at 3
		student.deleteBlock(3);
		
		for(Record<Student> record: student) {
			System.out.println(record);
		}
		
		student.save();
	}
	
	private static String convertLine(String line) {
		String replaced = line;
		
		replaced = replaced.replaceAll("'", "");
		replaced = replaced.replaceAll(", ", ",");
		
		return replaced;
	}

	private static <T extends Tuple> Relation<T> loadCSV(String relationName, Supplier<T> tupleMaker) {
		return loadCSV(relationName, tupleMaker, TestDelete::convertLine);
	}

	private static <T extends Tuple> Relation<T> loadCSV(String relationName, Supplier<T> tupleMaker, Function<String, String> convertLine) {
		Relation<T> relation = new Relation<T>(relationName, tupleMaker);

		relation.importFromFile(relationName + ".csv", convertLine);
		
		return relation;
	}
}

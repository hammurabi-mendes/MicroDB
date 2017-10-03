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

public class TestSelect {
	public static void main(String[] args) {
		Block.SIZE = 512;

		Queriable<Student> student = new Relation<>("student", () -> new Student());
		
		// Prints all students
		for(Record<Student> record: student) {
			System.out.println(record);
		}
		
		System.out.println("---------------");

		PrimaryIndex<Student, String> studentIndex1 = new DensePrimaryIndexTree<Student, String>(student, (tuple) -> tuple.name);
		SecondaryIndex<Student, String> studentIndex2 = new DenseSecondaryIndexTree<Student, String>(student, (tuple) -> tuple.department);
		
		Selector<Student> selector = new Selector<Student>(student);
		
		System.out.println(selector.selectOne(studentIndex1, "Lucas"));

		System.out.println("---------------");

		for(Record<Student> record: selector.selectMatchingKeys(studentIndex2, "Comp. Sci.")) {
			System.out.println(record);
		}

		System.out.println("---------------");

		/*
(63, 16) [44258,Steinmetz,Accounting,28]
(64, 0) [81896,Feldman,Finance,46]
...
(116, 1) [91799,Steinmetz,Civil Eng.,96]
(116, 2) [10727,Allard,Physics,27]
(116, 3) [64169,Lucas,Civil Eng.,27]
(116, 4) [81031,Nanda,Psychology,56]
(116, 5) [18941,Denecker,History,46]
(116, 6) [46981,Yalk,Statistics,117]
		 */

		// Tested selectPredicate;
		// Tested abortOnFalse;
		for(Record<Student> record: selector.selectFollowingPredicate(
				studentIndex1, "Steinmetz", (t) -> !t.name.equals("Denecker"), true)) {
			System.out.println(record);
		}

	}
}

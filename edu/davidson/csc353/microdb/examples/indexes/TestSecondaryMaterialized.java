/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.indexes;

import edu.davidson.csc353.microdb.files.Block;
import edu.davidson.csc353.microdb.files.Relation;

import edu.davidson.csc353.microdb.indexes.*;

import edu.davidson.csc353.microdb.examples.files.college.*;

import edu.davidson.csc353.microdb.files.Queriable;

public class TestSecondaryMaterialized {
	public static void main(String[] args) {
		Block.SIZE = 512;

		Queriable<Student> student = new Relation<>("student", () -> new Student());

		System.out.println("---------------");

		SecondaryIndex<Student, String> studentIndex = new SecondaryIndexMaterialized<Student, String>(student, (tuple) -> tuple.department, k -> k, k -> k);

		for(RecordLocation location: studentIndex.get("Astrophysiology")) { // Prints nothing
			System.out.println(location);
		}

		System.out.println("---------------");

		/* Results for querying the Comp. Sci. department, below
(1, 10)
(2, 7)
(3, 4)
(3, 14)
(4, 11)
(4, 13)
(8, 0)
(9, 2)
(9, 10)
(10, 2)
(12, 13)
(13, 0)
(13, 11)
(14, 7)
(18, 3)
(18, 13)
(19, 7)
(19, 8)
(19, 14)
(20, 12)
(21, 8)
(21, 14)
(22, 0)
(22, 8)
(23, 9)
(26, 8)
(29, 13)
(31, 14)
(31, 15)
(33, 7)
(34, 16)
(35, 1)
(38, 4)
(40, 16)
(41, 1)
(44, 16)
(45, 6)
(46, 6)
(47, 3)
(48, 11)
(49, 2)
(51, 9)
(52, 0)
(52, 2)
(52, 9)
(53, 3)
(53, 16)
(55, 0)
(56, 0)
(56, 10)
(58, 6)
(58, 9)
(58, 13)
(114, 6)
(114, 12)
		 */
		for(RecordLocation location: studentIndex.get("Comp. Sci.")) { // Should print the results above
			System.out.println(location);
		}
	}
}

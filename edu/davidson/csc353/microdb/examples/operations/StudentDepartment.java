/*
 * Author: Hammurabi Mendes
 * License: BSD-3-Clause
 * 
 * Implemented for CSC353 (Database Systems) at Davidson College.
 */
package edu.davidson.csc353.microdb.examples.operations;

import edu.davidson.csc353.microdb.examples.files.college.Department;
import edu.davidson.csc353.microdb.examples.files.college.Student;
import edu.davidson.csc353.microdb.files.Tuple;

public class StudentDepartment implements Tuple {
	private Student student;
	private Department department;

	public StudentDepartment(Student student, Department department) {
		this.student = student;
		this.department = department;
	}

	public void load(String input) {
		String[] fields = input.split("#");
		
		student.load(fields[0]);
		department.load(fields[1]);
	}

	public String save() {
		return student.save() + "#" + department.save();
	}

	public int getSize() {
		return save().getBytes().length;
	}

	public String toString() {
		return "[" + save() + "]";
	}
}

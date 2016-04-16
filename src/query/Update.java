package query;

import heap.HeapFile;
import parser.AST_Update;
import relop.Schema;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

	private String tableName;
	
	private Object[] values;
	
	private Schema schema;
	
	private HeapFile heapFile;
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {

  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Update implements Plan

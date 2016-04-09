package query;

import index.HashIndex;
import global.Minibase;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {
	
	//AST_DropIndex tree;
	//AST_DropTable table;
	private String fileName;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {
	//this.tree = tree;
	//tree.jjtOpen();
	  fileName = tree.getFileName();
	  QueryCheck.indexExists(fileName);
	//public DropIndex(AST_DropIndex tree) throws QueryException
  }  

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // print the output message;
	  new HashIndex(fileName).deleteFile();
	  Minibase.SystemCatalog.dropIndex(fileName);
	  System.out.println("Index dropped.");

  } // public void execute()

} // class DropIndex implements Plan

package query;

import global.Minibase;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_CreateIndex;
import parser.ParseException;
import relop.Schema;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

	  /** Name of the table to create. */
	  protected String fileName;

	  /** Schema of the table to create. */
	  protected Schema schema;
	  
	  protected String tableName;
	  
	  protected String columnName;
	  
	  
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
	  
	    fileName = tree.getFileName();
	    tableName = tree.getIxTable();
	    schema = QueryCheck.tableExists(tableName);
	    columnName = tree.getIxColumn();
	    
	    QueryCheck.fileNotExists(fileName);
	    QueryCheck.indexExists(fileName);
	    QueryCheck.columnExists(schema, tree.getIxColumn());
	  
	  
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  
	new HashIndex(fileName);  
	  
	Minibase.SystemCatalog.createIndex(fileName, tableName, columnName);
	  
    // print the output message
    System.out.println("Index created.");

  } // public void execute()

} // class CreateIndex implements Plan

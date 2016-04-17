package query;

import java.util.ArrayList;

import index.HashIndex;
import global.Minibase;
import global.RID;
import heap.HeapFile;
import parser.AST_Delete;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Selection;
import relop.Tuple;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {
	
	private String tableName;
	private Predicate[][] pred;
	private Schema schema;
	private HeapFile heapFile;
	private int length;
	
	private IndexDesc[] indexes;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
	  this.tableName = tree.getFileName();
	  this.schema = QueryCheck.tableExists(tableName);
	  this.pred = tree.getPredicates();
	  this.length = pred.length;
	 
	  QueryCheck.predicates(schema, pred);
	  this.heapFile	= new HeapFile(tableName);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  
	  IndexDesc[] inds = Minibase.SystemCatalog.getIndexes(tableName);
	  //RID rid = Minibase.SystemCatalog.getFileRID(tableName, true);
	  //byte[] b = heapFile.selectRecord(rid);
	  
	  FileScan scan = new FileScan(schema, heapFile);
	  
	  while(scan.hasNext()){
		  boolean pass = true;
		  Tuple tup = scan.getNext();
		  //evaluate tuple
		  for(int i = 0; i < pred.length; i++){
			  for(int j = 0; j < pred[i].length; j++){
				  if(!pred[i][j].evaluate(tup)){
					  pass = false;
				  }
			  }
		  }
		  //if passes all pred, delete from file
		  if(pass == true){
			  RID rid = scan.getLastRID();
			  heapFile.deleteRecord(rid);
		  }
	  }
	  
	  scan.close();
	  
	  //need to also drop it from the catalog (Recreate the indexes)
		 indexes = Minibase.SystemCatalog.getIndexes(tableName);
		 for (int i = 0; i < indexes.length; i++){
			 IndexDesc tempIndexDesc = indexes[i];
			 String columnName = tempIndexDesc.columnName;
			 String tableName = tempIndexDesc.tableName;
			 String indexName = tempIndexDesc.indexName;
			 
			 Minibase.SystemCatalog.dropIndex(indexName);
			 Minibase.SystemCatalog.createIndex(indexName, tableName, columnName);
		 }
	    
	  
    // print the output message
    System.out.println("Row(s) deleted.");

  } // public void execute()

} // class Delete implements Plan

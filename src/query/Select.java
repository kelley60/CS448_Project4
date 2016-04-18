package query;

import java.util.ArrayList;

import global.Minibase;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
	
	//protected String fileName;
	//protected Predicate[][] pred;
	//protected Schema schema;
	//protected String[] cols;
	protected Integer[] fldnos;
	//protected String[] tables;
	protected SortKey[] sortkeys;
	protected HeapFile heapFile;
	static protected ArrayList<Iterator> selList;
	static protected ArrayList<Iterator> joinList;
	static protected ArrayList<Iterator> tableList;
	protected Iterator root;


  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
	  
	  //initialize
	  
	  
	  
	String[] tables = tree.getTables(); 
	//THIS AS WELL
	Predicate[][] pred = tree.getPredicates();
	
	//
	 
	String[] cols = tree.getColumns();
	
	
	System.out.println("The number of columns is " + cols.length);
	
	this.fldnos = new Integer[cols.length];
	this.tableList = new ArrayList<Iterator>();
	this.joinList = new ArrayList<Iterator>();
	this.selList = new ArrayList<Iterator>();
	
	//tree.g
	this.sortkeys = tree.getOrders();
	
	
	//create the tree
	
	//FileScan scan = new FileScan(this.schema, heapFile);
	
	//get selections
	//ArrayList<Selection> selList = new ArrayList<Selection>();
	ArrayList<Iterator> tableJoinList = new ArrayList<Iterator>();
	ArrayList<Schema> schemaList = new ArrayList<Schema>();
	//Boolean canSelect = true;
	
	//get the tables
	for(int i = 0; i < tables.length; i++){
		System.out.print("getting tableName:" + tables[i] + "\n");
		
		//check if able to select
		/*for(int j = 0; j < pred.length; j++){
			for(int k = 0; k < pred[j].length; k++){
				if(!pred[j][k].validate(tempSchema)){
					canSelect = false;
					break;
				}
			}
			if(canSelect){
				HeapFile tempFile = new HeapFile(tables[i]);
				FileScan temp = new FileScan(tempSchema, tempFile);
				Selection sel = new Selection(temp,pred[j]); 
				selList.add(sel);
			}
		}*/
		HeapFile tempFile = new HeapFile(tables[i]);
		Schema tempSchema = QueryCheck.tableExists(tables[i]);
		
		
		FileScan temp = new FileScan(tempSchema, tempFile);
		schemaList.add(tempSchema);
		//tableJoinList.add(temp);
		tableList.add(temp);
	}
	
	//cross join the tables together (for now)
	//need to determine the order of the joins later

	boolean beginJoin = false;
	
	int tableSize = tableList.size();
	int tableIndex = 2;
	int joinIndex = 0;
	int count = 0;
	ArrayList<Schema> joinSchema = new ArrayList<Schema>();
	//Schema nSchema = new Schema(0);
	System.out.println("Count is :" + count);
	System.out.print("tableList size is :" + tableSize + "\n");
	while(count < tableSize - 1){
		if(beginJoin == false){
			//join the first 2 tables together
			//System.out.print("joinList size:" + joinList.size() + "\n");
			//if(pred.length == 0){
			//	break;
			//}
			Schema nSchema = Schema.join(tableList.get(0).getSchema(), tableList.get(1).getSchema());
			for(int i = 0; i < cols.length;i++){
				QueryCheck.columnExists(nSchema, cols[i]);
			}
			SimpleJoin curJoin = new SimpleJoin(tableList.get(0), tableList.get(1),pred[0]);
			joinList.add(curJoin); // the first join
			System.out.print("joinList size2:" + joinList.size() + "\n");
			
			//nSchema.print();
			joinSchema.add(nSchema);
			beginJoin = true;
			count++;
		}
		else{
			//join the rest with the next table and the previous join iterator
			//last schema on joinSchema List should be the final schema
			System.out.print("joinList size2:" + joinList.size() + "\n");
			SimpleJoin curJoin2 = new SimpleJoin(tableList.get(tableIndex), joinList.get(joinIndex),pred[0]);
			joinList.add(curJoin2);
			Schema nSchema2 = Schema.join(tableList.get(tableIndex).getSchema(), schemaList.get(joinIndex));
			joinSchema.add(nSchema2);
			nSchema2.print();
			tableIndex++;
			joinIndex++;
			count++;
		}
	}
	
	//select from the join
	//ArrayList<Iterator> prevSel = new ArrayList<Iterator>();
	
	if(tableList.size() == 1){
		joinSchema.add(tableList.get(0).getSchema());
	}
	int selCount;
	boolean beginSel = false;
	int prevSel = 0;
	for(int i = 0; i < pred.length; i++){
		if(beginSel == false){
			//select using the last join
			//System.out.print("joinList size:" + joinList.size() + "\n");
			int joinSchemaLastIndex = joinSchema.size()-1;
			System.out.println("Last index of join schema is " + joinSchemaLastIndex);
			QueryCheck.predicates(joinSchema.get(joinSchemaLastIndex), pred);
			if(tableList.size() != 1){
				Selection tempSel = new Selection(joinList.get(joinList.size()-1),pred[i]);
				selList.add(tempSel);
			}
			else{
				Selection tempSel = new Selection(tableList.get(0),pred[i]);
				selList.add(tempSel);
			}
		}
		else{
			//use the previous selects to create the next join
			Selection tempSel2 = new Selection(selList.get(prevSel),pred[i]);
			QueryCheck.predicates(joinSchema.get(joinSchema.size()-1), pred);
			selList.add(tempSel2);
			prevSel++;
		}
	}
	System.out.print("selList size:" + selList.size() + "\n");
	
	//finally project with the last selection of the list
	
	for(int i = 0; i < cols.length; i++){
		fldnos[i] = QueryCheck.columnExists(joinSchema.get(joinSchema.size()-1), cols[i]);
	}
	
	//mutiple tables and SELECT *
	if (tableList.size() > 1 && cols.length == 0){
		
		Schema lastSchema = joinSchema.get(joinSchema.size() - 1);
		int lastSchemaFieldCount = lastSchema.getCount();
		
		this.fldnos = new Integer[lastSchemaFieldCount];
		
		System.out.println("Number of columns in the last schema is " + lastSchemaFieldCount);
		for(int i = 0; i < lastSchemaFieldCount; i++){
			fldnos[i] = QueryCheck.columnExists(lastSchema, lastSchema.fieldName(i));
		}
	}
	//SELECT * FROM 1 table with predicates
	
	// SElECT * FROM 1 Table without predicates
	if(cols.length == 0 && pred.length == 0){
		this.root = tableList.get(0);
	}
	//no predicates to select from
	else if(selList.size() == 0){
		this.root = new Projection(tableList.get(0),fldnos);
	}
	//select from multiple tables with given predicates
	else{
		System.out.print("projecting\n");
		if(fldnos.length == 0){
			this.root = selList.get(selList.size()-1);
		}
		else{
			this.root = new Projection(selList.get(selList.size()-1),fldnos);
		}
	}
	
	
	
	
	
  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // print the output message
	   int selected = root.execute();
	  System.out.print(selected  + " row(s) seleceted\n");
    
  } // public void execute()

} // class Select implements Plan

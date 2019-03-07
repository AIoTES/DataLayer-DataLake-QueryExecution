package eu.activage.datalake.query;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

class Query {
	private String sql;
	String index;
	String[] columns;
	String [] conditions;
	private final Logger logger = LoggerFactory.getLogger(Query.class);
	
	Query(String sql) throws Exception{
		// Parse query
		Statement stmt = CCJSqlParserUtil.parse(sql);
		this.sql = sql;
	    		
		// Get data
		if(stmt instanceof Select){
			Select select = (Select) stmt;
		    				
			// Get table names
			index = getTableNames(select).get(0); // Only one table
			logger.info("Index: " + index);
			
			// Get column names
			columns = getColumnNames(select); // TODO: add support to expressions?
            logger.info("Column names: ");
            for(int i=0; i<columns.length; i++){
            	logger.info(columns[i]);
            }
			
			// Get conditions
            logger.info("Conditions: ");
            try{
            	conditions = getAndConditions(select); // Only simple conditions using AND are supported
            }catch(NullPointerException e){
            	conditions = null;
            	logger.warn("No conditions");
            }
             
         // TODO: add more condition operators
            
          				
		}else{
			throw new Exception("Query not supported: " + sql);
		}
	
		
	}
	
   // Query parsing methods
    
    protected List<String> getTableNames(Select select){
    	TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    	List<String> tableList = tablesNamesFinder.getTableList(select);
    	return tableList;
    }
    
    protected String[] getColumnNames(Select select) throws Exception{	
    	PlainSelect plain=(PlainSelect)select.getSelectBody();     
        List selectitems=plain.getSelectItems();
        logger.info(selectitems.size() + " query items identified");
        String[] columns = new String[selectitems.size()];
        for(int i=0;i<selectitems.size();i++){
        	// TODO: add support to *
           Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();  
           logger.info("Expression: " + expression.toString());
           if( expression instanceof Column){
               // A column name
        	   Column col=(Column)expression;
 //              logger.info(col.getTable()+","+col.getColumnName());      
               columns[i] = col.getColumnName();
               // Do something with col.getTable() to support queries over multiple tables

           }else if (expression instanceof Function){
        	   // An expression
               Function function = (Function) expression;
               logger.info(function.getAttribute()+","+function.getName()+""+function.getParameters()); 
               // TODO
               throw new Exception("Expressions are not supported");
           }
        }
    	
    	return columns;
    }
    
    // TODO: Add support to other possible types of conditions (OR, brackets, etc)
    
    protected String[] getAndConditions(Select select){
    	// Get conditions with AND
    	List<String> conditions = new ArrayList<String>();
    	PlainSelect ps = (PlainSelect) select.getSelectBody();
    	
    	Expression expr = ps.getWhere();
    	expr.accept(new ExpressionVisitorAdapter() {

            @Override
            protected void visitBinaryExpression(BinaryExpression expr) {
                if (expr instanceof ComparisonOperator) {
                	String value = expr.getRightExpression().toString();
                	value = value.replaceAll("'", "");
                	String condition = expr.getLeftExpression() + " " + expr.getStringExpression() + " " + value;
                	conditions.add(condition);
                   System.out.println("left=" + expr.getLeftExpression() + "  op=" +  expr.getStringExpression() + "  right=" + expr.getRightExpression());
                }
                super.visitBinaryExpression(expr); 
            }
            
            @Override
            public void visit(AndExpression expr) {
                expr.getLeftExpression().accept(this);
                expr.getRightExpression().accept(this);

            }
 //           @Override
//            public void visit(OrExpression expr) {
//            }
//            @Override
//            public void visit(Parenthesis parenthesis) {                
//            }
            
        });
    	
    	return conditions.toArray(new String[conditions.size()]);
    	
    }
    
    String createQueryString() throws JSQLParserException{
    	// TODO: check syntax
    	String q = null;
    	
    	Statement statement = CCJSqlParserUtil.parse(sql);
    	
    	if(statement instanceof Select){
//    		q = "SELECT ";
//        	for (int i = 0; i < columns.length; i++){
//    			q = q + columns[i];
//    			if(i==columns.length-1) q = q + " ";
//    			else q = q + ", ";
//    		}
//    		q = q + "FROM " + index;
    		
    		// For independent data storage.
    		q = "SELECT * ";
    		q = q + "FROM " + columns[0];
    		       	
        	
        	PlainSelect ps = (PlainSelect)((Select)statement).getSelectBody();
        	
        	Expression expr = ps.getWhere();
        	List<String> conditions = new ArrayList<String>();
        	if(expr != null){
        		expr.accept(new ExpressionVisitorAdapter() {
            		int depth = 0;
                    public void processLogicalExpression( BinaryExpression expr, String logic){
                    	depth++;
                        expr.getLeftExpression().accept(this);
                        conditions.add(logic);
                        expr.getRightExpression().accept(this);
                        if(  depth != 0 ){
                            depth--;
                        }
                    }

                    @Override
                    protected void visitBinaryExpression(BinaryExpression expr) {
                        if (expr instanceof ComparisonOperator) {
                        	conditions.add(expr.getLeftExpression() +  " " + expr.getStringExpression() + " " + expr.getRightExpression());
                        	
                        } 
                        super.visitBinaryExpression(expr); 
                    }

                    @Override
                    public void visit(AndExpression expr) {
                        processLogicalExpression(expr, "AND");

                    }
                    @Override
                    public void visit(OrExpression expr) {
                        processLogicalExpression(expr, "OR");
                    }
                    @Override
                    public void visit(Parenthesis parenthesis) {
                    	conditions.add("(");
                        parenthesis.getExpression().accept(this);
                        conditions.add(")");
                    }
                    
                });
        	}
        	
        	if(!conditions.isEmpty()){
        		q = q + " WHERE";
            	
            	for(String c : conditions){
            		q = q + " " + c;
            	}
        	}
        	        	        	
    	}
    	
    	return q;
    }
    
    // Test
    public String toString(){
    	    	    	
    	Gson gson = new Gson();
    	JsonObject structure = new JsonObject();
    	structure.addProperty("Table", index);
    	structure.addProperty("Columns", gson.toJson(columns));
    	structure.addProperty("Conditions", gson.toJson(conditions));
    	// Add more attributes  	
    	
    	return structure.toString();
    }
    
    String getQuery(){
    	return sql;
    }
    
}
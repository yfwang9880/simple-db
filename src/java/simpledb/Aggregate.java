package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    
    private OpIterator child;
    private int afield;
    private  int gfield;
    private Aggregator.Op aop;
    private OpIterator iterator;
    
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	
    	this.child=child;
    	this.afield=afield;
    	this.gfield=gfield;
    	this.aop=aop;
    	iterator=null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	
    	super.open();
    	child.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	
    	if(iterator==null){
    		if(!child.hasNext())
    			return null;
    		
    		Aggregator agg;
    		Type gfieldtype = (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldType(gfield);
        	if(child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)){
        		agg=new IntegerAggregator(gfield, gfieldtype, afield, aop);
        	}
        	else{
        		agg=new StringAggregator(gfield, gfieldtype, afield, aop);
        	}
        	
        	while(child.hasNext()){
        		Tuple t=child.next();
        		agg.mergeTupleIntoGroup(t);
        	}
        	
        	iterator=agg.iterator();
        	iterator.open(); //////// 
    	}
    	
    	while(iterator.hasNext())
    		return iterator.next();
    	
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	close();
    	open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	TupleDesc td=child.getTupleDesc();
    	Type[] typeAr;
    	String[] fieldAr;
    	
    	if(gfield==-1){
    		typeAr=new Type[]{Type.INT_TYPE};
    		fieldAr=new String[]{this.aggregateFieldName()};
    		return new TupleDesc(typeAr,fieldAr);
    	}
    	else{
    		Type gfieldtype = child.getTupleDesc().getFieldType(gfield);
    		typeAr=new Type[]{gfieldtype,Type.INT_TYPE};
    		fieldAr=new String[]{this. groupFieldName(),this.aggregateFieldName()};
    		return new TupleDesc(typeAr,fieldAr);
    	}
	
    }

    public void close() {
	// some code goes here
    	super.close();
    	child.close();
    	iterator=null;
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
    	
    	child=children[0];
    }
    
}

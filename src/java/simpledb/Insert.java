package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private boolean called;
    
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	
    	this.t=t;
    	this.child=child;
    	this.tableId=tableId;
    	
    	TupleDesc td1=child.getTupleDesc();
    	TupleDesc td2=Database.getCatalog().getTupleDesc(tableId);
    	
    	if(!td1.equals(td2)) 
    		throw new NoSuchElementException();
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] typeAr=new Type[]{Type.INT_TYPE};
    	String[] fieldAr=new String[]{"number of inserts"};
    	TupleDesc td=new TupleDesc( typeAr,fieldAr);
    	
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	
    	super.open();
    	child.open();
    	called=false;
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    	called=true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        
    	if(called)
    		return null;
    	called=true;
    	int count=0;
    	
    	while(child.hasNext()){
    		try{
    			Database.getBufferPool().insertTuple(t, tableId, child.next());
    			count++;
    		}
    		catch(IOException e){
    			
    		}
    	}
    	
    	Type[] typeAr=new Type[]{Type.INT_TYPE};
    	String[] fieldAr=new String[]{"number of inserts"};
    	TupleDesc td=new TupleDesc( typeAr,fieldAr);
    	
    	Tuple t=new Tuple(td);
    	t.setField(0, new IntField(count));
    	
    	return t;
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

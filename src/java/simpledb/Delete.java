package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    
    private TransactionId t;
    private OpIterator child;
    
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	
    	this.t=t;
    	this.child=child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] typeAr=new Type[]{Type.INT_TYPE};
    	String[] fieldAr=new String[]{"number of deletes"};
    	TupleDesc td=new TupleDesc( typeAr,fieldAr);
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	
    	close();
    	open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	int count=0;
        while(child.hasNext()){
        	try{
    			Database.getBufferPool().deleteTuple(t, child.next());
    			count++;
    		}
    		catch(IOException e){
    			
    		}
        }
        
        Type[] typeAr=new Type[]{Type.INT_TYPE};
    	String[] fieldAr=new String[]{"number of deletes"};
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

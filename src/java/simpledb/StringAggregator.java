package simpledb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    private Map<Field,Integer> mapV;
    private Map<Field,Integer> mapC;
    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
    	this.mapV=new HashMap<>();
    	switch(what) {
    	case COUNT:
    		break;
    	default:
    		throw new IllegalArgumentException();    			
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    	Field groupValue;
    	if(gbfield!=-1)
    	    groupValue=tup.getField(gbfield);
    	else groupValue=null;
    	Field aValue=tup.getField(afield);
    	
    	if(mapV.containsKey(groupValue)){
    		mapV.put(groupValue, mapV.get(groupValue)+1);
    	}
    	else{
    		mapV.put(groupValue, 1);
    	}
    }
    

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        
    	List<Tuple> list=new LinkedList<>();
    	TupleDesc td;
        if(gbfield==-1){
        	td=new TupleDesc(new Type[] {Type.INT_TYPE});
        }
        else
        	td=new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        
        if(gbfield==-1){
        	for(Field key: mapV.keySet()){
        		Tuple t=new Tuple(td);
        		int value=mapV.get(key);
        		t.setField(0, new IntField(value));
        		list.add(t);
        	}
        }
        else{
        	for(Field key: mapV.keySet()){
        		Tuple t=new Tuple(td);
        		int value=mapV.get(key);
        		t.setField(0, key);
        		t.setField(1, new IntField(value));
        		list.add(t);
        	}
        }
        
        return new TupleIterator(td,list);
    	
    }

}

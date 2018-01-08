package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	
	public File file;
	private TupleDesc tDesc;
	//private HashMap<Integer, HeapPage> pMap;
	private RandomAccessFile raf;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	
        // some code goes here
    	this.file = f;
    	this.tDesc = td;
    	try {
    		raf = new RandomAccessFile(f, "rw");
    	}catch(FileNotFoundException e) {
    		e.printStackTrace();
    	}
    	
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return tDesc;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int offset = pid.getPageNumber() * BufferPool.getPageSize();
    	byte[] pData = new byte[BufferPool.getPageSize()];
    	//Page p;
    	try {
    		raf.seek(offset);
    		raf.read(pData, 0, BufferPool.getPageSize());
    		return new HeapPage((HeapPageId) pid, pData);
    		
    	}catch(IOException e) {
    		e.printStackTrace();
    	}
		
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	
    		int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
    		RandomAccessFile rf = new RandomAccessFile(file, "rw");
    		rf.seek(offset);
        rf.write(page.getPageData());
        rf.close();
    	
    	/*
    	int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
    	
    	try {
    		raf.write(page.getPageData(), offset, BufferPool.getPageSize());
    	}
    	catch(Exception e) {
    		throw new IOException(e.getMessage());
    	}*/
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	
        // some code goes here
    	    ArrayList<Page> pList = new ArrayList<Page>();
    	    int pgNo;
    		for(pgNo = 0; pgNo < this.numPages(); pgNo++) {
    			HeapPageId pid = new HeapPageId(this.getId(), pgNo);
    			HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    			if(p.getNumEmptySlots() > 0) {
    				p.insertTuple(t);
    				pList.add(p);
    				return pList;
    			}

    		}
    		HeapPageId pid = new HeapPageId(this.getId(), pgNo);
    		byte[] bytes = new byte[BufferPool.getPageSize()];
    		HeapPage p = new HeapPage(pid, bytes);

    		p.insertTuple(t);
    		this.writePage(p);
    		pList.add(p);
    		return pList;/*
    	int pageNo;
    	for(pageNo = 0; pageNo < this.numPages(); pageNo++) {
    		HeapPageId pid = new HeapPageId(getId(), pageNo);
    		HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    		if(p.getNumEmptySlots() > 0){
        		break;
        	}
    	}
    if(pageNo == this.numPages()) {
    	HeapPageId 	pid = new HeapPageId(getId(), pageNo);
    	byte[] bytes = new byte[BufferPool.getPageSize()];
		HeapPage hp = new HeapPage(pid, bytes);
		writePage(hp);
    }
    HeapPageId 	pid = new HeapPageId(getId(), pageNo);
		
		
		
    HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	
    	p.insertTuple(t);
    	ArrayList<Page> ls = new ArrayList<Page>();
    	ls.add(p);
        return ls;*/
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    		ArrayList<Page> pList = new ArrayList<Page>();
    		RecordId rid = t.getRecordId();
    		HeapPageId pid = (HeapPageId) rid.getPageId();
    		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    		p.deleteTuple(t);
    		pList.add(p);
        return pList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(tid, this);
    	
    }

}

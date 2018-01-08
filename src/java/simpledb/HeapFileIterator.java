package simpledb;

import java.util.*;
import java.io.*;

public class HeapFileIterator implements DbFileIterator {
	
	List<Tuple> tupleList = new ArrayList<>();
	Iterator<Tuple> tpIterator = null;
	TransactionId tid;
	Page currentPage;
	int pgNo = 0;
	int tableId;
	int numPg;
	
	
	public HeapFileIterator(TransactionId tid, HeapFile file) {
		this.tid = tid;
		this.tableId = file.getId();
		numPg = file.numPages();
	}
	
	public void open() throws DbException, TransactionAbortedException {
		HeapPageId pid = new HeapPageId(tableId, pgNo++);
		currentPage = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		tpIterator = ((HeapPage) currentPage).iterator();
	}
	
	/*
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if(tpIterator == null) {
			return false;
		}
		else if(tpIterator.hasNext()) {
			return true;
		}
		else {
			if(pgNo < numPg) {
				int newPgNo = pgNo + 1;
				HeapPageId newPid = new HeapPageId(tableId, newPgNo);
				Page newPage = Database.getBufferPool().getPage(tid, newPid, Permissions.READ_ONLY);
				Iterator<Tuple> newIterator = ((HeapPage) newPage).iterator();
				if(newIterator.hasNext())
					return true;
			}
			
		}
		return false;
	}*/
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (tpIterator == null)
			return false;
		if (tpIterator.hasNext())
			return true;
		while (pgNo < numPg) {
			HeapPageId pid = new HeapPageId(tableId, pgNo++);
			currentPage = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
			tpIterator = ((HeapPage) currentPage).iterator();
			if (tpIterator.hasNext()) {
				return true;
			}
			//else
			//	return false;
		}
		return false;
	}
	
	/*
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if(tpIterator == null) {
			throw new NoSuchElementException("Tuple iterator not opened");
		}
		else if(tpIterator.hasNext()) {
			return tpIterator.next();
		}
		else {
			if(pgNo < numPg) {
				pgNo++;
				HeapPageId pid = new HeapPageId(tableId, pgNo);
				currentPage = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
				tpIterator = ((HeapPage) currentPage).iterator();
				if(tpIterator.hasNext())
					return tpIterator.next();
			}
		}
		return null;
	}*/
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if (tpIterator == null) {
			throw new NoSuchElementException("Tuple iterator not opened");
		}
		//assert (tpIterator.hasNext());
		return tpIterator.next();
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		this.close();
		this.open();
	}
	
	public void close() {
		pgNo = 0;
		tpIterator = null;
	}
	
}
package simpledb;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */




public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096; // 4096

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 500;

    // page buffer; PageId -> page
    private ConcurrentHashMap<PageId, Page> pgBufferPool;
    private int capacity;

    private LockManager lockMgr;
    private static int TRANSATION_FACTOR = 2;
    // timeout 1s for deadlock detection
    private static int DEFAUT_MAXTIMEOUT = 5000;
    // int size;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.capacity = numPages;
        this.pgBufferPool = new ConcurrentHashMap<PageId, Page>();
        this.lockMgr = new LockManager(numPages, TRANSATION_FACTOR * numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        LockManager.LockType lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = LockManager.LockType.SLock;
        } else {
            lockType = LockManager.LockType.XLock;
        }
        Debug.log(pid.toString() + ": before acquire lock\n");
        lockMgr.acquireLock(tid, pid, lockType, DEFAUT_MAXTIMEOUT);
        Debug.log(pid.toString() + ": acquired the lock\n");

        Page pg;
        if (pgBufferPool.containsKey(pid)) {
            pg = pgBufferPool.get(pid);
        } else {
            if (pgBufferPool.size() >= capacity) {
                evictPage();
            }
            pg = Database
                    .getCatalog()
                    .getDatabaseFile(pid.getTableId())
                    .readPage(pid);
            pgBufferPool.put(pid, pg);
        }
        return pg;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockMgr.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockMgr.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            // flush all the pages into disk
            flushPages(tid);
        }

        // abort and commited, discard all the pages
        // just invalidate all the pages in tid
        // invalidateCache(tid);

        ArrayList<PageId> lockList = lockMgr.getLockList(tid);
        if (lockList != null) {
            for (PageId pid : lockList) {
                Page pg = pgBufferPool.getOrDefault(pid, null);
                if (pg != null && pg.isDirty() != null) {
                    // all dirty pages are flushed and not dirty page are still in cache
                    discardPage(pid);
                }
            }
        }

        // release locks finally
        lockMgr.releaseLocksOnTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affected = tableFile.insertTuple(tid, t);
        for (Page newPg : affected) {
            newPg.markDirty(true, tid);
            pgBufferPool.remove(newPg.getId());
            pgBufferPool.put(newPg.getId(), newPg);
        }
    } 

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        DbFile tableFile = Database
                            .getCatalog()
                            .getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> affected = tableFile.deleteTuple(tid, t);
        for (Page newPg : affected) {
            newPg.markDirty(true, tid);
            pgBufferPool.remove(newPg.getId());
            pgBufferPool.put(newPg.getId(), newPg);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Enumeration<PageId> it = pgBufferPool.keys();
        while (it.hasMoreElements()) {
            flushPage(it.nextElement());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pgBufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (pgBufferPool.containsKey(pid)) {
            Page pg = pgBufferPool.get(pid);
            if (pg.isDirty() != null) {
                // then write back
            	Database.getLogFile().logWrite(pg.isDirty(), pg.getBeforeImage(), pg);
                Database.getLogFile().force();
                DbFile tb = Database.getCatalog().getDatabaseFile(pg.getId().getTableId());
                tb.writePage(pg);
                pg.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        ArrayList<PageId> page2flush = lockMgr.getLockList(tid);
        if (page2flush != null) {
            for (PageId p : page2flush) {
                flushPage(p);
                // use current page contents as the before-image
                // for the next transaction that modifies this page.
                pgBufferPool.get(p).setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // randomly discard a page
        for (Map.Entry<PageId, Page> entry : pgBufferPool.entrySet()) {
            PageId pid = entry.getKey();
            Page   p   = entry.getValue();
            if (p.isDirty() == null) {
                // dont need to flushpage since all page evicted are not dirty
                // flushPage(pid);
                discardPage(pid);
                return;
            }
        }
        throw new DbException("BufferPool: evictPage: all pages are marked as dirty");
    }

}

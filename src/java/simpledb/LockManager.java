package simpledb;
import java.util.ArrayList;
import java.util.Random;

import simpledb.LockManager.LockType;
import simpledb.LockManager.ObjLock;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

public class LockManager {
    enum LockType {
        SLock, XLock
    }

    class ObjLock {
        // boolean blocked;
        LockType type;
        PageId obj;
        ArrayList<TransactionId> holders;

        /*
        public boolean isBlocked() {
            return blocked;
        }
        public void setBlocked(boolean blocked) {
            this.blocked = blocked;
        }
        */

        public ObjLock(LockType t, PageId obj, ArrayList<TransactionId> holders) {
            // this.blocked = false;
            this.type = t;
            this.obj = obj;
            this.holders = holders;
        }

        public void setType(LockType type) {
            this.type = type;
        }

        public LockType getType() {
            return type;
        }

        public PageId getObj() {
            return obj;
        }

        public ArrayList<TransactionId> getHolders() {
            return holders;
        }

        public boolean tryUpgradeLock(TransactionId tid) {
            if (type == LockType.SLock && holders.size() == 1 && holders.get(0).equals(tid)) {
                type = LockType.XLock;
                return true;
            }
            return false;
        }

        public TransactionId addHolder(TransactionId tid) {
            if (type == LockType.SLock) {
                if (!holders.contains(tid)) {
                    holders.add(tid);
                }
                return tid;
            }
            return null;
        }
    }

    private ConcurrentHashMap<PageId, ObjLock> lockTable;
    private ConcurrentHashMap<TransactionId, ArrayList<PageId>> transactionTable;

    public LockManager(int lockTabCap, int transTabCap) {
        this.lockTable = new ConcurrentHashMap<>(lockTabCap);
        this.transactionTable = new ConcurrentHashMap<>(transTabCap);
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        ArrayList<PageId> lockList = getLockList(tid);
        return lockList != null && lockList.contains(pid);
    }

    private synchronized void block(PageId what, long start, long timeout)
            throws TransactionAbortedException {
        // activate blocking
        // lockTable.get(what).setBlocked(true);

        if (System.currentTimeMillis() - start > timeout) {
            // System.out.println(Thread.currentThread().getId() + ": aborted");
            throw new TransactionAbortedException();
        }

        try {
            wait(timeout);
            if (System.currentTimeMillis() - start > timeout) {
                // System.out.println(Thread.currentThread().getId() + ": aborted");
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            /* do nothing */
            e.printStackTrace();
        }
    }

    private synchronized void updateTransactionTable(TransactionId tid, PageId pid) {
        if (transactionTable.containsKey(tid)) {
            if (!transactionTable.get(tid).contains(pid)) {
                transactionTable.get(tid).add(pid);
            }
        } else {
            // no entry tid
            ArrayList<PageId> lockList = new ArrayList<PageId>();
            lockList.add(pid);
            transactionTable.put(tid, lockList);
        }
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType reqLock, int maxTimeout)
            throws TransactionAbortedException {
        // boolean isAcquired = false;
        long start = System.currentTimeMillis();
        Random rand = new Random();
        long randomTimeout = rand.nextInt((maxTimeout - 0) + 1) + 0;
        while (true) {
            if (lockTable.containsKey(pid)) {
                // page is locked by some transaction
                if (lockTable.get(pid).getType() == LockType.SLock) {
                    if (reqLock == LockType.SLock) {
                        updateTransactionTable(tid, pid);
                        assert lockTable.get(pid).addHolder(tid) != null;
                        // isAcquired = true;
                        return;
                    } else {
                        // request XLock
                        if (transactionTable.containsKey(tid) && transactionTable.get(tid).contains(pid)
                                && lockTable.get(pid).getHolders().size() == 1) {
                            // sanity check
                            assert lockTable.get(pid).getHolders().get(0) == tid;
                            // this is a combined case when lock on pid hold only by one trans (which is exactly tid)
                            lockTable.get(pid).tryUpgradeLock(tid);
                            // isAcquired = true;
                            return;
                        } else {
                            // all need to do is just blocking
                            block(pid, start, randomTimeout);
                        }
                    }
                } else {
                    // already get a Xlock on pid
                    if (lockTable.get(pid).getHolders().get(0) == tid) {
                        // Xlock means only one holder
                        // request xlock or slock on the pid with that tid
                        // sanity check
                        assert lockTable.get(pid).getHolders().size() == 1;
                        // isAcquired = true;
                        return;
                    } else {
                        // otherwise block
                        block(pid, start, randomTimeout);
                    }
                }
            } else {
                ArrayList<TransactionId> initialHolders = new ArrayList<>();
                initialHolders.add(tid);
                lockTable.put(pid, new ObjLock(reqLock, pid, initialHolders));
                updateTransactionTable(tid, pid);
                // isAcquired = true;
                return;
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {

        // remove from trans table
        if (transactionTable.containsKey(tid)) {
            transactionTable.get(tid).remove(pid);
            if (transactionTable.get(tid).size() == 0) {
                transactionTable.remove(tid);
            }
        }

        // remove from locktable
        if (lockTable.containsKey(pid)) {
            lockTable.get(pid).getHolders().remove(tid);
            if (lockTable.get(pid).getHolders().size() == 0) {
                // no more threads are waiting here
                lockTable.remove(pid);
            } else {
                // ObjLock lock = lockTable.get(pid);
                // synchronized (lock) {
                notifyAll();
                //}
            }
        }
    }

    public synchronized void releaseLocksOnTransaction(TransactionId tid) {
        if (transactionTable.containsKey(tid)) {
            PageId[] pidArr = new PageId[transactionTable.get(tid).size()];
            PageId[] toRelease = transactionTable.get(tid).toArray(pidArr);
            for (PageId pid : toRelease) {
                releaseLock(tid, pid);
            }

        }
    }

    public synchronized ArrayList<PageId> getLockList(TransactionId tid) {
        return transactionTable.getOrDefault(tid, null);
    }
}
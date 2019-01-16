import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;
    private HashMap<LockType, HashMap<LockType, Boolean>> compMatrix;

    public LockManager() {
        this.resourceToLock = new HashMap<>();
        compMatrix = new HashMap<>();
        compMatrix.put(LockType.S, new HashMap<>());
        compMatrix.put(LockType.X, new HashMap<>());
        compMatrix.put(LockType.IS, new HashMap<>());
        compMatrix.put(LockType.IX, new HashMap<>());
        compMatrix.get(LockType.S).put(LockType.S, true);
        compMatrix.get(LockType.S).put(LockType.X, false);
        compMatrix.get(LockType.S).put(LockType.IS, true);
        compMatrix.get(LockType.S).put(LockType.IX, false);
        compMatrix.get(LockType.X).put(LockType.S, false);
        compMatrix.get(LockType.X).put(LockType.X, false);
        compMatrix.get(LockType.X).put(LockType.IS, false);
        compMatrix.get(LockType.X).put(LockType.IX, false);
        compMatrix.get(LockType.IS).put(LockType.S, true);
        compMatrix.get(LockType.IS).put(LockType.X, false);
        compMatrix.get(LockType.IS).put(LockType.IS, true);
        compMatrix.get(LockType.IS).put(LockType.IX, true);
        compMatrix.get(LockType.IX).put(LockType.S, false);
        compMatrix.get(LockType.IX).put(LockType.X, false);
        compMatrix.get(LockType.IX).put(LockType.IS, true);
        compMatrix.get(LockType.IX).put(LockType.IX, true);
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {
        // HW5: To do

        // 1st error case - done
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }

        //2nd error case - done
        if (holds(transaction, resource, lockType)) {
            throw new IllegalArgumentException();
        }

        //3rd error case - done
        if (lockType == LockType.S && holds(transaction, resource, LockType.X)) {
            throw new IllegalArgumentException();
        }

        //4th error case - done
        if (resource.getResourceType() == Resource.ResourceType.TABLE) {
            if (lockType == LockType.IS && holds(transaction, resource, LockType.IX)) {
                throw new IllegalArgumentException();
            }
        }

        //5th error case - done
        if (resource.getResourceType() == Resource.ResourceType.PAGE
                && (lockType == LockType.IS || lockType == LockType.IX)) {
            throw new IllegalArgumentException();
        }

        //6th error case - done
        if (resource.getResourceType() == Resource.ResourceType.PAGE) {
            ResourceLock rTable = resourceToLock.get(((Page) resource).getTable());
            boolean check = false;
            if (rTable != null) {
                ArrayList<Request> owners = rTable.lockOwners;
                if (!owners.isEmpty()) {
                    if (lockType == LockType.S) {
                        for (Request req : owners) {
                            if (req.transaction.equals(transaction) && (req.lockType == LockType.IS || req.lockType == LockType.IX)) {
                                check = true;
                            }
                        }
                    } else if (lockType == LockType.X) {
                        for (Request req : owners) {
                            if (req.transaction.equals(transaction) && req.lockType == LockType.IX) {
                                check = true;
                            }
                        }
                    }
                    if (!check) {
                        throw new IllegalArgumentException();
                    }
                } else {
                    throw new IllegalArgumentException();
                }
            } else {
                throw new IllegalArgumentException();
            }
        }

        // acquire code
        Request r = new Request(transaction, lockType);

        if (resourceToLock.get(resource) == null) {
            resourceToLock.put(resource, new ResourceLock());
        }
        ResourceLock rLocks = resourceToLock.get(resource);

        if (compatible(resource, transaction, lockType)) {
            boolean contains = false;
            boolean isStoX = false;
            if (lockType == LockType.X) {
                for (Request owner : rLocks.lockOwners) {
                    if (owner.transaction.equals(transaction)) {
                        contains = true;
                        if (owner.lockType == LockType.S) {
                            isStoX = true;
                        }
                    }
                }
            }

            if (contains && isStoX) {
                for (Request owner : rLocks.lockOwners) {
                    if (owner.transaction.equals(transaction)) {
                        owner.lockType = lockType;
                    }
                }
            } else {
                rLocks.lockOwners.add(r);
            }
        } else {
            boolean priority = false;
            if (lockType == LockType.X) {
                for (Request owner : rLocks.lockOwners) {
                    if (owner.transaction.equals(transaction) && owner.lockType == LockType.S) {
                        priority = true;
                    }
                }
            }

            if (priority) {
                rLocks.requestersQueue.add(0, r);
            } else  {
                rLocks.requestersQueue.add(r);
            }
            r.transaction.sleep();
        }
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {
        // HW5: To do
        // done
        ResourceLock resourceLock = resourceToLock.get(resource);
        if (resourceLock != null) {
            ArrayList<Request> owners = resourceToLock.get(resource).lockOwners;
            for (Request req : owners) {
                if (req.transaction.equals(transaction)) {
                    if (!(req.lockType == LockType.S && lockType == LockType.X)) {
                        return false;
                    }
                } else if (!compMatrix.get(req.lockType).get(lockType)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{
        // HW5: To do

        // 1st error case - done
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }

        // 2nd error case - done
        boolean lockS = holds(transaction, resource, LockType.S);
        boolean lockX = holds(transaction, resource, LockType.X);
        boolean lockIS = holds(transaction, resource, LockType.IS);
        boolean lockIX = holds(transaction, resource, LockType.IX);
        if (!(lockS || lockX || lockIS || lockIX)) {
            throw new IllegalArgumentException();
        }

        // 3rd error case - done
        if (resource.getResourceType() == Resource.ResourceType.TABLE) {
            for (Page p : ((Table) resource).getPages()) {
                ResourceLock rLock = resourceToLock.get(p);
                if (rLock != null) {
                    for (Request owner : rLock.lockOwners) {
                        if (owner.transaction == transaction) {
                            throw new IllegalArgumentException();
                        }
                    }
                }
            }
        }

        // release code
        ResourceLock rLock = resourceToLock.get(resource);
        for (Request owner : rLock.lockOwners) {
            if (owner.transaction == transaction) {
                rLock.lockOwners.remove(owner);
                break;
            }
        }

        promote(resource);
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         // HW5: To do
         // done
         ResourceLock rLock = resourceToLock.get(resource);
         if (!(rLock.requestersQueue.isEmpty())) {
             while (!rLock.requestersQueue.isEmpty()) {
                 if (compatible(resource, rLock.requestersQueue.peek().transaction, rLock.requestersQueue.peek().lockType)) {
                     Request next = rLock.requestersQueue.poll();
                     next.transaction.wake();
                     acquire(next.transaction, resource, next.lockType);
                 } else {
                     break;
                 }
             }
         }
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        // HW5: To do
        // done
        ResourceLock rLock = resourceToLock.get(resource);
        if (rLock != null) {
            ArrayList<Request> owners = rLock.lockOwners;
            for (Request owner : owners) {
                if (owner.transaction.equals(transaction) && owner.lockType == lockType) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}

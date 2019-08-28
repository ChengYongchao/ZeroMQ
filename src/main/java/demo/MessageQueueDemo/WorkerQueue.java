package demo.MessageQueueDemo;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/*
 * Worker队列
 * */
public class WorkerQueue {
    private List<Worker> workers = null;

    public WorkerQueue() {
        workers = new ArrayList<>();
    }

    public Boolean addWorker(ZFrame address) {
        if (address == null) {
            return false;
        }
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker oldworker = it.next();
            if (address.equals(oldworker.address)) {
                it.remove();
            }
        }
        Worker worker = new Worker(address);
        workers.add(worker);
        return true;
    }

    public ZFrame next() {


        Worker worker = workers.get(0);
        if (worker == null) {
            return null;
        }
        workers.remove(0);
        workers.add(worker);
        return worker.address;
    }

    public int size() {
        return workers.size();
    }

    //worker对象
    private class Worker {
        public ZFrame address = null;  //  Address of worker
        String identity = ""; //  Printable identity


        public Worker(ZFrame address) {
            this.address = address;
        }
    }
}


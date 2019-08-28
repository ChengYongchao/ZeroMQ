package demo.MessageQueueDemo;

import org.zeromq.ZFrame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*
 * Worker队列
 * */
public class WorkerQueue {
    private List<Worker> workers = null;
    public Boolean addWorker = true; //是否允许继续添加worker

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
                return false;
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
        return new ZFrame(worker.address.getData());
    }

    public int size() {
        return workers.size();
    }

    public Boolean setWorkerStatus(ZFrame address, int status) {
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker worker = it.next();
            if (address.equals(worker.address)) {
                worker.status = status;
                it.remove();
                workers.add(worker);
                return true;
            }
        }
        return false;
    }

    /*
     *设置worker拿到的是哪个顶点
     *  */
    public Boolean setWorkerIndex(ZFrame address, int index) {
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker worker = it.next();
            if (address.equals(worker.address)) {
                worker.index = index;
                it.remove();
                workers.add(worker);
                return true;
            }
        }
        return false;
    }

    public Boolean allWorkerStatus(int status) {
        if (workers.size() == 0) {
            return false;
        }
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker worker = it.next();
            if (worker.status != status) {
                return false;
            }
        }
        return true;
    }

    public Boolean hasWorkerStatus(int status) {
        if (workers.size() == 0) {
            return false;
        }
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker worker = it.next();
            if (worker.status == status) {
                return true;
            }
        }
        return false;
    }

    public Boolean isReady() {
        if (workers.size() == 5 && allWorkerStatus(0)) {
            addWorker = false;
            return true;
        }
        return false;
    }

    //worker对象
    private class Worker {
        public ZFrame address = null;  //  Address of workers
        public int index = -1; //worker拿到的顶点集的index
        public Boolean hasVertex = false; //是否分到了顶点集
        public int status = -1; //节点状态 -1：离线；0:存活且未分到顶点; 1：存活且分到顶点但是未执行图计算任务；2：存活且执行图计算任务中；

        //String identity = ""; //  Printable identity


        public Worker(ZFrame address) {
            this.address = address;
            status = 0;
        }

        public Worker(ZFrame address, int status) {
            this.address = address;
            this.status = status;
        }

    }
}


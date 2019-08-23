package demo.MessageQueueDemo;

import org.zeromq.*;

import java.security.Policy;


//worker DEALER
public class worker implements Runnable {

    ZContext context = null;

    private final static String PPP_READY = "\001";

    worker(ZContext cxt) {
        this.context = cxt;
    }

    @Override
    public void run() {
        try {
            System.out.println("Worker is started:");
            ZMQ.Socket worker = context.createSocket(SocketType.DEALER);
            worker.connect("tcp://*:5556");
            ZMsg readymsg = new ZMsg();
            readymsg.add(new ZFrame(PPP_READY));
            readymsg.send(worker);

            ZMQ.Poller poller = context.createPoller(1);
            poller.register(worker, ZMQ.Poller.POLLIN);

            while (true) {
                int rc = poller.poll(1000);
                if (rc == -1)
                    break; //  Interrupted

                if (poller.pollin(0)) {
                    ZMsg receivedmsg = ZMsg.recvMsg(worker);
                    if (receivedmsg == null) break;

                    //todo 消息处理机制
                    if (receivedmsg.size() == 2) {
                        String task = new String(receivedmsg.pop().getData(), ZMQ.CHARSET);
                        String data = new String(receivedmsg.pop().getData(), ZMQ.CHARSET);
                        System.out.println("task: " + task + " " + "data: " + data);
                    }


                }
            }

        } catch (Exception e) {

        }
    }

    public static void main(String[] args) {
        ZContext context = new ZContext();
        //Thread thread1 = new Thread(new worker(context));
        //thread1.start();
        for(int num = 100;num > 0; num--){
            new Thread(new worker(context)).start();
        }
    }
}

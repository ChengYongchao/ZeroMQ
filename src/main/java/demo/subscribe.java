package demo;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class subscribe implements Runnable {
    ZContext context;
    ZMQ.Socket subscriber;
    String filter = "";

    private subscribe() {
        context = new ZContext(1);
        subscriber = context.createSocket(SocketType.SUB);
    }

    @Override
    public void run() {
        try {
            int index = 1;
            while (true) {
                //  Socket to talk to server
                subscriber.connect("tcp://localhost:5556");
                subscriber.subscribe(filter.getBytes());
                System.out.println("beginning subscribe:");
                String message = subscriber.recvStr(0).trim();
                System.out.println("ReceiveMessage=======>" + message + "<======第" + index + "条");
                //if(index == 1000) break;
                index++;
            }
        } catch (Exception e) {
            System.out.println(System.err);
        } finally {
            subscriber.close();
            context.close();
        }
    }

    public static void main(String[] args) {
        subscribe sub = new subscribe();
        Thread thread1 = new Thread(sub);
        sub.run();
    }
}

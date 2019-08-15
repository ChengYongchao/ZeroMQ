package demo;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class publish {
    public static void main(String[] args) {
        ZContext context = new ZContext(1);
        ZMQ.Socket publisher = context.createSocket(SocketType.PUB);

        try {
            publisher.bind("tcp://*:5556");
            publisher.bind("ipc://weather");
            int index = 1;
            while (true) {
                String message = "hello world!";
                publisher.send(message.getBytes(ZMQ.CHARSET), ZMQ.NOBLOCK);
                System.out.println("PublishMessage=======> hello world!<====第" + index + "条");
                //if(index == 1000) break;
                index ++;
            }
        } catch (Exception e) {
            System.out.println(System.err);
        } finally {
            publisher.close();
            context.close();
        }
    }
}

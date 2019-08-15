package demo;
import org.zeromq.ZMQ;
public class server {
    public static void main(String[] args) throws InterruptedException {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket responder = context.socket(ZMQ.REP);
        responder.bind("tcp://*:5555");
        while (!Thread.currentThread().isInterrupted()) {
            byte[] request = responder.recv(0);
            System.out.println("Received" + new String(request));
            Thread.sleep(1);
            String reply = "World";
            responder.send(reply.getBytes(),0);
        }
        responder.close();
        context.term();
    }
}

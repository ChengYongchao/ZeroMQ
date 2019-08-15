package demo;
import org.zeromq.ZMQ;
public class client {
    public static void main(String[] args) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket requester = context.socket(ZMQ.REQ);
        requester.connect("tcp://localhost:5555");
        for (int requestNbr = 0; requestNbr != 1000; requestNbr++) {
            String request = "Hello";
            System.out.println("Sending Hello: " + requestNbr);
            requester.send(request.getBytes(),0);
            byte[] reply = requester.recv(0);
            System.out.println("Reveived: " + new String(reply) + " " + requestNbr);
        }
        requester.close();
        context.term();
    }
}

package directory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Directory Service class that registers brokers and provides information about available brokers
 * to publishers and subscribers.
 */
public class DirectoryService {
    // List to store broker information
    private ArrayList<HashMap<String, String>> brokerList = new ArrayList<>();

    /**
     * Main method to start the Directory Service.
     * It listens for incoming connections from brokers, publishers, and subscribers.
     *
     * @param args Command-line arguments. The first argument is the port number of the directory service.
     */
    public static void main(String[] args) {
        int portNumber = Integer.parseInt(args[0]);
        DirectoryService directoryService = new DirectoryService();

        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            System.out.println("Directory Service is listening on port " + portNumber);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client is connected");
                DirectoryHandler handler = new DirectoryHandler(clientSocket, directoryService);
                handler.start();
            }
        } catch (IOException e) {
            System.out.println("Directory Service failed to start");
            e.printStackTrace();
        }
    }

    /**
     * Registers a broker with the directory service.
     *
     * @param brokerIp   The IP address of the broker.
     * @param brokerPort The port number of the broker.
     */
    public synchronized void registerBroker(String brokerIp, String brokerPort) {
        HashMap<String, String> brokerData = new HashMap<>();
        brokerData.put("brokerIp", brokerIp);
        brokerData.put("brokerPort", brokerPort);
        this.brokerList.add(brokerData);
    }

    /**
     * Provides the list of all active brokers.
     *
     * @return A JSON array containing broker information (IP and port).
     */
    public synchronized JSONArray getBrokerList() {
        JSONArray brokerArray = new JSONArray();
        for (HashMap<String, String> broker : brokerList) {
            JSONObject brokerJson = new JSONObject();
            brokerJson.put("brokerIp", broker.get("brokerIp"));
            brokerJson.put("brokerPort", broker.get("brokerPort"));
            brokerArray.add(brokerJson);
        }
        return brokerArray;
    }

}

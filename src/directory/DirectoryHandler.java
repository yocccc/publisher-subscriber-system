package directory;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * DirectoryHandler handles client connections to the directory service.
 * It processes broker registration and broker list requests from publishers and subscribers.
 */
public class DirectoryHandler extends Thread {
    private Socket clientSocket;
    private DirectoryService directoryService;

    public DirectoryHandler(Socket clientSocket, DirectoryService directoryService) {
        this.clientSocket = clientSocket;
        this.directoryService = directoryService;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
             PrintWriter writer = new PrintWriter(this.clientSocket.getOutputStream(), true)) {

            String line;
            while ((line = reader.readLine()) != null) {
                JSONParser parser = new JSONParser();
                JSONObject request = (JSONObject) parser.parse(line);

                String userType = (String) request.get("user type");
                if ("broker".equals(userType)) {
                    //Register a new broker by adding it to the directory's broker list.
                    String brokerIp = (String) request.get("brokerIp");
                    String brokerPort = (String) request.get("brokerPort");
                    this.directoryService.registerBroker(brokerIp, brokerPort);
                    JSONObject response = new JSONObject();
                    response.put("user type", "directory");
                    response.put("brokers", this.directoryService.getBrokerList());
                    writer.println(response.toJSONString());
                }
                else if ("publisher".equals(userType) || "subscriber".equals(userType)) {
                    //Sends the list of active brokers to the publisher or subscriber.
                    JSONObject response = new JSONObject();
                    response.put("brokers", this.directoryService.getBrokerList());
                    writer.println(response.toJSONString());
                }
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

}

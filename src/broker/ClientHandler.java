package broker;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.*;
import java.io.*;

/**
 * Handles communication with a connected client (publisher, subscriber, or
 * broker).
 * This class is responsible for receiving and processing requests from clients,
 * performing the requested actions, and sending responses back to the client.
 */
public class ClientHandler extends Thread {
    private Socket socket;
    private Broker broker;

    /**
     * Constructor to initialize the client handler with a socket and a broker
     * reference.
     *
     * @param socket the client's socket connection
     * @param broker the broker instance that manages topics, publishers, and
     *               subscribers
     */
    public ClientHandler(Socket socket, Broker broker) {
        this.socket = socket;
        this.broker = broker;
    }

    /**
     * The main logic of the client handler.
     * It processes messages from the client (publisher, subscriber, or broker) and
     * performs actions
     * such as creating topics, publishing messages, subscribing to topics, etc.
     */
    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
            JSONParser parser = new JSONParser();

            // Read initial user information (e.g., user name, type)
            JSONObject userInfo = (JSONObject) parser.parse(reader.readLine());
            String userName = (String) userInfo.get("user name");
            String userType = (String) userInfo.get("user type");

            // Register the client as a subscriber, publisher, or broker
            if (userType != null) {
                if (userType.equals("subscriber")) {
                    this.broker.addSubscriberSocket(userName, this.socket);
                } else if (userType.equals("publisher")) {
                    this.broker.addPublisherSocket(userName, this.socket);
                } else if (userType.equals("broker")) {
                    String brokerIp = (String) userInfo.get("ip address");
                    int brokerPort = Integer.parseInt((String) userInfo.get("port number"));
                    this.broker.connectToOtherBroker(brokerIp, brokerPort);
                }
            }

            String line;
            // Continuously read and process requests from the client
            while ((line = reader.readLine()) != null) {
                JSONObject request = (JSONObject) parser.parse(line);

                // if -d option is used, connect to other brokers.
                if (request.containsKey("user type") && request.get("user type").equals("broker")) {
                    String brokerIp = (String) userInfo.get("ip address");
                    int brokerPort = Integer.parseInt((String) userInfo.get("port number"));
                    this.broker.connectToOtherBroker(brokerIp, brokerPort);
                } else {
                    String command = (String) request.get("command");
                    // Handle the command from the client
                    JSONObject response = handleRequest(command, request, userName);
                    if (response != null) {
                        writer.println(response.toJSONString()); // Send response to the client
                    }
                }

            }
            // Handle client disconnection
            if (line == null) {
                System.out.println(userType);
                if (userType.equals("publisher")) {
                    this.broker.deleteAllTopicByPublisher(userName);
                } else if (userType.equals("subscriber")) {
                    this.broker.deleteAllTopicBySubscriber(userName);
                }
                System.out.println("Client disconnected.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ParseException e) {
            e.getMessage();
        }
    }

    /**
     * Handles a command sent by the client.
     * This method processes different commands such as creating topics, publishing
     * messages,
     * subscribing/unsubscribing to topics, and returning responses based on the
     * actions performed.
     *
     * @param command  the command to be executed (e.g., "create", "publish",
     *                 "subscribe")
     * @param request  the JSON request object containing details for the command
     * @param userName the name of the user (publisher or subscriber) issuing the
     *                 command
     * @return a JSONObject containing the result of the command execution (success
     *         or failure)
     */
    private JSONObject handleRequest(String command, JSONObject request, String userName) {
        switch (command) {
            case "create":
                return this.broker.createTopic((String) request.get("topic id"), (String) request.get("topic name"),
                        userName);
            case "publish":
                return this.broker.publishMessage((String) request.get("topic id"), (String) request.get("message"),
                        userName);
            case "countSubscriber":
                return this.broker.countSubscribers(userName);
            case "delete":
                return this.broker.deleteTopic((String) request.get("topic id"), userName);
            case "list":
                return this.broker.listTopics();
            case "subscribe":
                return this.broker.subscribe((String) request.get("topic id"), userName);
            case "unsubscribe":
                return this.broker.unsubscribe((String) request.get("topic id"), userName);
            case "showCurrentSubscription":
                return this.broker.showCurrentSubscription(userName);
            case "sync":
                this.broker.handleSyncMessage(request); // Handle synchronization messages from other brokers
                return null;
            default:
                JSONObject response = new JSONObject();
                response.put("result", "failed");
                response.put("detail", "Invalid command.");
                return response;
        }
    }
}

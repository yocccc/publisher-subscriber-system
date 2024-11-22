/** @author Yoshikazu Fujisaka SN:1414141)
 */

package broker;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Broker class that handles the communication between publishers, subscribers,
 * and other brokers.
 * It can register itself with a Directory Service using the '-d' option, or run
 * independently.
 * It manages topics, subscribers, and publishers, allowing creation, deletion,
 * and message publishing.
 * Additionally, it synchronizes topic creation and deletion across connected
 * brokers.
 */
public class Broker {
    private int portNumber; // Port number for the broker
    private String ipAddress; // IP Address for the broker
    private HashMap<String, String> topicList = new HashMap<>(); // Maps topic IDs to topic titles
    private HashMap<String, String> publisherTopic = new HashMap<>(); // Maps topic IDs to publisher names
    private HashMap<String, HashSet<String>> subscriberTopic = new HashMap<>(); // Maps subscriber names to a set of
                                                                                // topic IDs they are subscribed to
    private HashMap<String, Socket> subscriberSockets = new HashMap<>(); // Maps subscriber names to their socket
                                                                         // connections
    private HashMap<String, Socket> publisherSockets = new HashMap<>(); // Maps publisher names to their socket
                                                                        // connections
    private List<Socket> connectedBrokerSockets = new ArrayList<>(); // List of connected broker sockets

    /**
     * Constructor for Broker class. Initializes the broker with the specified port
     * number.
     *
     * @param portNumber Port number where the broker will listen for incoming
     *                   connections.
     */
    public Broker(int portNumber) {
        this.portNumber = portNumber;
        try {
            this.ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.out.println("Failed to retrieve the IP address");
        }
    }

    /**
     * Main method to start the Broker server.
     * If "-b" is provided as a command-line argument, it will connect to other
     * brokers.
     * If "-d" is provided as a command-line argument, it will register with a
     * Directory Service.
     *
     * @param args Command-line arguments. The first argument is the port number.
     *             Optional: "-b" followed by IP:Port of other brokers.
     *             Optional: "-d" followed by Directory Service IP and port to
     *             register with the directory.
     */
    public synchronized static void main(String[] args) {
        int portNumber = Integer.parseInt(args[0]);
        Broker broker = new Broker(portNumber);

        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            // Broker starts listening first
            System.out.println("Broker is listening on port " + portNumber);

            // If directory service information is provided, register the broker
            if (args.length > 1 && args[1].equals("-d")) {
                String[] directoryAddress = args[2].split(":");
                String directoryIp = directoryAddress[0];
                int directoryPort = Integer.parseInt(directoryAddress[1]);
                broker.registerWithDirectory(directoryIp, directoryPort);
            }

            // Connect to other brokers only after starting the server socket
            if (args.length > 1 && args[1].equals("-b")) {
                for (int i = 2; i < args.length; i++) {
                    String[] brokerAddress = args[i].split(":");
                    String brokerIp = brokerAddress[0];
                    int brokerPort = Integer.parseInt(brokerAddress[1]);
                    // Use a new thread to connect to other brokers asynchronously
                    broker.connectToOtherBroker(brokerIp, brokerPort);

                }
            }

            // Accept incoming connections from clients (publishers, subscribers, or
            // brokers)
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Publisher, Subscriber, or Broker is connected");
                ClientHandler clientHandler = new ClientHandler(clientSocket, broker);
                clientHandler.start();
            }
        } catch (IOException e) {
            System.out.println("Failed to start broker server: " + e.getMessage());
        }
    }

    /**
     * Registers this broker with the Directory Service.
     * The broker sends its IP address and port number to the directory service.
     *
     * @param directoryIp   The IP address of the Directory Service.
     * @param directoryPort The port number of the Directory Service.
     */
    public synchronized void registerWithDirectory(String directoryIp, int directoryPort) {
        try (Socket socket = new Socket(directoryIp, directoryPort);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            JSONObject registerMessage = new JSONObject();
            registerMessage.put("user type", "broker");
            registerMessage.put("brokerIp", this.ipAddress);
            registerMessage.put("brokerPort", this.portNumber + "");
            writer.println(registerMessage.toJSONString());
            // Retrieve the list of other brokers from the directory service
            String response = reader.readLine();

            JSONParser parser = new JSONParser();
            JSONObject directoryResponse = (JSONObject) parser.parse(response);

            // Get the list of brokers from the directory response
            JSONArray brokers = (JSONArray) directoryResponse.get("brokers");
            if (brokers != null && !brokers.isEmpty()) {
                // Connect to each broker in the list
                for (Object brokerObj : brokers) {
                    JSONObject brokerInfo = (JSONObject) brokerObj;
                    String brokerIp = (String) brokerInfo.get("brokerIp");
                    int brokerPort = Integer.parseInt((String) brokerInfo.get("brokerPort"));
                    if (brokerIp.equals(this.ipAddress) && brokerPort == this.portNumber) {
                        continue; // Skip connecting to self
                    }
                    // Connect to the broker
                    connectToOtherBroker(brokerIp, brokerPort);
                }
            }
        } catch (IOException | ParseException e) {
            System.out.println("Failed to register with Directory Service.");
        }
    }

    /**
     * Connects the current broker to another broker specified by IP and port.
     * This ensures that the two brokers can synchronize their topics and
     * subscribers.
     *
     * @param brokerIp   The IP address of the broker to connect to.
     * @param brokerPort The port number of the broker to connect to.
     * @throws IOException If there is an error during the connection.
     */
    public synchronized void connectToOtherBroker(String brokerIp, int brokerPort) throws IOException {
        // Check if the broker is already connected

        for (Socket socket : connectedBrokerSockets) {
            if (socket.getInetAddress().getHostAddress().equals(brokerIp) && socket.getPort() == brokerPort) {
                return; // Already connected
            }
        }

        // Connect to the broker
        Socket brokerSocket = new Socket(brokerIp, brokerPort);
        this.connectedBrokerSockets.add(brokerSocket);

        // Send connection details to the connected broker
        JSONObject brokerInfo = new JSONObject();
        brokerInfo.put("user type", "broker");
        brokerInfo.put("port number", this.portNumber + "");
        brokerInfo.put("ip address", this.ipAddress);
        PrintWriter writer = new PrintWriter(brokerSocket.getOutputStream(), true);
        writer.println(brokerInfo.toJSONString());
        System.out.println("Connected to broker at " + brokerIp + ":" + brokerPort);
    }

    /**
     * Adds a subscriber's socket to the subscriberSockets map.
     * This is used to track which socket is associated with which subscriber.
     *
     * @param subscriberName The name of the subscriber.
     * @param socket         The socket connection for the subscriber.
     */
    public synchronized void addSubscriberSocket(String subscriberName, Socket socket) {
        this.subscriberSockets.put(subscriberName, socket);
    }

    /**
     * Adds a publisher's socket to the publisherSockets map.
     * This is used to track which socket is associated with which publisher.
     *
     * @param publisherName The name of the publisher.
     * @param socket        The socket connection for the publisher.
     */
    public synchronized void addPublisherSocket(String publisherName, Socket socket) {
        this.publisherSockets.put(publisherName, socket);
    }

    /**
     * Creates a new topic with the given ID and name, associating it with the
     * publisher.
     * If the topic ID already exists, the creation fails. The topic creation is
     * also
     * synchronized with other brokers in the network.
     *
     * @param topicId   The unique ID of the topic.
     * @param topicName The name of the topic.
     * @param publisher The name of the publisher creating the topic.
     * @return A JSONObject containing the result of the topic creation.
     */
    public synchronized JSONObject createTopic(String topicId, String topicName, String publisher) {
        JSONObject jsonObject = new JSONObject();
        if (this.topicList.containsKey(topicId)) {
            jsonObject.put("result", "failed");
            jsonObject.put("detail", "Topic ID already exists.. use another one");
        } else {
            this.topicList.put(topicId, topicName);
            this.publisherTopic.put(topicId, publisher);
            this.syncCreateTopicWithOtherBrokers(topicId, topicName, publisher);
            jsonObject.put("result", "success");
            jsonObject.put("detail", "Topic created successfully.");
        }
        return jsonObject;
    }

    /**
     * Synchronizes the creation of a topic with other brokers in the network.
     * This method sends a sync message to all connected brokers to ensure the new
     * topic
     * is reflected across the entire broker network.
     *
     * @param topicId   The unique ID of the topic.
     * @param topicName The name of the topic.
     * @param publisher The name of the publisher who created the topic.
     */
    private synchronized void syncCreateTopicWithOtherBrokers(String topicId, String topicName, String publisher) {
        JSONObject syncMessage = new JSONObject();
        syncMessage.put("command", "sync");
        syncMessage.put("syncAction", "create");
        syncMessage.put("topic id", topicId);
        syncMessage.put("topic name", topicName);
        syncMessage.put("publisher", publisher);

        // Send the sync message to other brokers
        sendSyncMessageToOtherBrokers(syncMessage);
    }

    /**
     * Counts the number of subscribers for each topic associated with a given
     * publisher.
     * This method iterates through the topics published by the publisher and counts
     * how many
     * subscribers are subscribed to each topic.
     *
     * @param publisher The name of the publisher whose topics are being queried.
     * @return A JSONObject containing the result and details of the subscriber
     *         counts.
     */
    public synchronized JSONObject countSubscribers(String publisher) {
        JSONObject response = new JSONObject();
        JSONArray subscribedCountArray = new JSONArray();
        boolean isExistTopic = false;

        // Iterate through each topic published by the given publisher
        for (HashMap.Entry<String, String> entry : this.publisherTopic.entrySet()) {
            if (entry.getValue() == null) {
                response.put("result", "failed");
                response.put("detail", "you have not created any topic");
                return response;
            }
            if (entry.getValue().equals(publisher)) {
                String topicId = entry.getKey();
                int subscriberCount = 0;

                // Count how many subscribers are subscribed to this topic
                for (HashSet<String> subscribedTopics : this.subscriberTopic.values()) {
                    if (subscribedTopics.contains(topicId)) {
                        subscriberCount++;
                    }
                }

                // Create a JSON object for the topic info
                JSONObject topicInfo = new JSONObject();
                topicInfo.put("topic id", topicId);
                topicInfo.put("title", this.topicList.get(topicId));
                topicInfo.put("count", subscriberCount + "");
                subscribedCountArray.add(topicInfo);
                isExistTopic = true;
            }
        }

        // Return the result in a JSON response
        if (isExistTopic) {
            response.put("result", "success");
            response.put("detail", subscribedCountArray);
        } else {
            response.put("result", "failed");
            response.put("detail", "you have not created any topic");
        }
        return response;
    }

    /**
     * Deletes a topic by its ID, ensuring that only the publisher who created the
     * topic can delete it.
     * After deletion, the method notifies all subscribers of the topic and
     * synchronizes the deletion with other brokers.
     *
     * @param topicId                 The ID of the topic to delete.
     * @param requestingPublisherName The name of the publisher requesting the
     *                                deletion.
     * @return A JSONObject containing the result of the deletion operation.
     */
    public synchronized JSONObject deleteTopic(String topicId, String requestingPublisherName) {
        JSONObject response = new JSONObject(); // Initialize response object

        // Check if the topic exists and if the requesting publisher is the original
        // publisher
        if (!this.topicList.containsKey(topicId) || !this.publisherTopic.containsKey(topicId)
                || !this.publisherTopic.get(topicId).equals(requestingPublisherName)) {
            response.put("result", "failed");
            response.put("detail", "you do not have this topic id.");
            return response;
        }

        String title = this.topicList.get(topicId); // Get the topic name
        String publisher = this.publisherTopic.get(topicId); // Get the publisher name

        // Remove the topic from the broker
        this.topicList.remove(topicId);
        this.publisherTopic.remove(topicId);

        // Remove the topic from subscribers and notify them
        for (Map.Entry<String, HashSet<String>> entry : this.subscriberTopic.entrySet()) {
            HashSet<String> subscribedTopics = entry.getValue();
            if (subscribedTopics.contains(topicId)) {
                String subscriber = entry.getKey();
                subscribedTopics.remove(topicId); // Remove from subscription list
                notifySubscriber(topicId, subscriber, title, publisher); // Notify subscriber
            }
        }

        // Synchronize the deletion with other brokers
        syncDeleteTopicWithOtherBrokers(topicId, publisher);
        response.put("result", "success");
        response.put("detail", "id: " + topicId + " has successfully been deleted.");
        return response;
    }

    /**
     * Notifies a subscriber that a topic they are subscribed to has been deleted.
     *
     * @param topicId    The ID of the deleted topic.
     * @param subscriber The name of the subscriber to notify.
     * @param title      The title of the deleted topic.
     * @param publisher  The name of the publisher who created the topic.
     */
    public synchronized void notifySubscriber(String topicId, String subscriber, String title, String publisher) {
        Socket subscriberSocket = subscriberSockets.get(subscriber);
        JSONArray deletedTopics = new JSONArray();
        if (subscriberSocket != null) {
            try {
                JSONObject message = new JSONObject();
                message.put("message type", "deleteNotify");
                JSONObject topicInfo = new JSONObject();
                topicInfo.put("topic id", topicId);
                topicInfo.put("title", title);
                topicInfo.put("publisher", publisher);
                deletedTopics.add(topicInfo);
                message.put("deleted topic", deletedTopics);
                PrintWriter writer = new PrintWriter(subscriberSocket.getOutputStream(), true);
                writer.println(message.toJSONString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Synchronizes the deletion of a topic with other brokers in the network.
     *
     * @param topicId   The ID of the deleted topic.
     * @param publisher The name of the publisher who created the topic.
     */
    private synchronized void syncDeleteTopicWithOtherBrokers(String topicId, String publisher) {
        JSONObject syncMessage = new JSONObject();
        syncMessage.put("command", "sync");
        syncMessage.put("syncAction", "delete");
        syncMessage.put("topic id", topicId);
        syncMessage.put("publisher", publisher);

        // Send the sync message to other brokers
        sendSyncMessageToOtherBrokers(syncMessage);
    }

    /**
     * Deletes all topics created by a specific publisher.
     * Notifies all subscribers and synchronizes the deletions with other brokers.
     *
     * @param publisher The name of the publisher whose topics should be deleted.
     */
    public synchronized void deleteAllTopicByPublisher(String publisher) {
        ArrayList<String> idToRemove = new ArrayList<>();

        // Collect all topic IDs to remove
        for (HashMap.Entry<String, String> entry : this.publisherTopic.entrySet()) {
            if (entry.getValue() != null && entry.getValue().equals(publisher)) {
                idToRemove.add(entry.getKey()); // Add topic IDs for deletion
            }
        }

        // Notify subscribers of the deleted topics
        for (HashMap.Entry<String, HashSet<String>> entry : subscriberTopic.entrySet()) {
            JSONObject message = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            String subscriber = "";
            message.put("message type", "deleteNotify");

            for (String topicId : idToRemove) {
                if (entry.getValue().contains(topicId)) {
                    String title = this.topicList.get(topicId);
                    subscriber = entry.getKey();
                    JSONObject topicInfo = new JSONObject();
                    topicInfo.put("topic id", topicId);
                    topicInfo.put("title", title);
                    topicInfo.put("publisher", publisher);
                    jsonArray.add(topicInfo);
                    subscriberTopic.get(subscriber).remove(topicId);
                }
            }

            message.put("deleted topic", jsonArray);
            if (!subscriber.isEmpty()) {
                notifySubscriber(message, subscriber);
            }
        }

        // Remove the topics from the topic list
        for (String topicId : idToRemove) {
            this.topicList.remove(topicId);
            this.publisherTopic.remove(topicId);
        }

        // Synchronize the deletion with other brokers
        syncDeleteAllTopicsByPublisherWithOtherBrokers(publisher, idToRemove);
    }

    /**
     * Notifies a subscriber that a topic they are subscribed to has been deleted.
     *
     * @param message    The message containing topic title and publisher name.
     * @param subscriber The name of the subscriber to notify.
     */
    private synchronized void notifySubscriber(JSONObject message, String subscriber) {
        Socket subscriberSocket = subscriberSockets.get(subscriber);

        // Check if the subscriber's socket exists
        if (subscriberSocket != null) {
            try {
                // Send the message to the subscriber
                PrintWriter writer = new PrintWriter(subscriberSocket.getOutputStream(), true);
                writer.println(message.toJSONString());
            } catch (IOException e) {
                // Handle potential I/O errors during message sending
                e.printStackTrace();
            }
        }
    }

    /**
     * Synchronizes the deletion of all topics by a specific publisher with other
     * brokers.
     *
     * @param publisher  The name of the publisher whose topics have been deleted.
     * @param idToRemove A list of topic IDs that were deleted.
     */
    private synchronized void syncDeleteAllTopicsByPublisherWithOtherBrokers(String publisher,
            ArrayList<String> idToRemove) {
        JSONObject syncMessage = new JSONObject();
        syncMessage.put("command", "sync");
        syncMessage.put("syncAction", "deleteAllTopicsByPublisher");
        syncMessage.put("deleted topics", idToRemove);
        syncMessage.put("publisher", publisher);

        // Send the sync message to other brokers
        sendSyncMessageToOtherBrokers(syncMessage);
    }

    /**
     * Publishes a message to a specific topic, sending it to all subscribers of
     * that topic.
     * The message is also synchronized with other brokers in the network.
     *
     * @param topicId   The ID of the topic to publish to.
     * @param message   The message to be published.
     * @param publisher The name of the publisher sending the message.
     * @return A JSONObject containing the result of the publishing operation.
     */
    public synchronized JSONObject publishMessage(String topicId, String message, String publisher) {
        JSONObject response = new JSONObject();
        if (!this.publisherTopic.containsKey(topicId) || !this.publisherTopic.get(topicId).equals(publisher)) {
            response.put("result", "failed");
            response.put("detail", "you don't have this topic id");
            return response;
        }

        ArrayList<String> subscribers = new ArrayList<>();
        String title = this.topicList.get(topicId);

        // Find all subscribers to the topic
        for (HashMap.Entry<String, HashSet<String>> entry : this.subscriberTopic.entrySet()) {
            String subscriber = entry.getKey();
            HashSet<String> idSet = entry.getValue();
            if (idSet.contains(topicId)) {
                subscribers.add(subscriber);
            }
        }

        // Send the message to all subscribers
        if (!subscribers.isEmpty()) {
            for (String subscriber : subscribers) {
                sendMessageToSubscriber(subscriber, message, title, publisher, topicId);
            }
        }

        // Synchronize the message with other brokers
        syncPublishMessageWithOtherBrokers(topicId, message, publisher);
        response.put("result", "success");
        response.put("detail", "message has been published!");
        return response;
    }

    /**
     * Sends a message to a specific subscriber of a topic.
     *
     * @param subscriber The name of the subscriber.
     * @param message    The message to send.
     * @param title      The title of the topic.
     * @param publisher  The name of the publisher sending the message.
     * @param topicId    The ID of the topic.
     */
    private synchronized void sendMessageToSubscriber(String subscriber, String message, String title, String publisher,
            String topicId) {
        Socket subscriberSocket = subscriberSockets.get(subscriber);
        if (subscriberSocket != null) {
            try {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("message type", "broadcast");
                jsonObject.put("publisher", publisher);
                jsonObject.put("title", title);
                jsonObject.put("topic id", topicId);
                jsonObject.put("message", message);

                PrintWriter writer = new PrintWriter(subscriberSocket.getOutputStream(), true);
                writer.println(jsonObject.toJSONString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Synchronizes the publishing of a message with other brokers in the network.
     *
     * @param topicId   The ID of the topic to which the message was published.
     * @param message   The message that was published.
     * @param publisher The name of the publisher who published the message.
     */
    private synchronized void syncPublishMessageWithOtherBrokers(String topicId, String message, String publisher) {
        JSONObject syncMessage = new JSONObject();
        syncMessage.put("command", "sync");
        syncMessage.put("syncAction", "publish");
        syncMessage.put("topic id", topicId);
        syncMessage.put("message", message);
        syncMessage.put("publisher", publisher);

        // Send the sync message to other brokers
        sendSyncMessageToOtherBrokers(syncMessage);
    }

    /**
     * Retrieves a list of all available topics, including the topic ID, title, and
     * publisher.
     *
     * @return A JSONObject containing the result and the list of topics.
     */
    public synchronized JSONObject listTopics() {
        JSONObject response = new JSONObject();
        JSONArray topicArray = new JSONArray();
        boolean exist = false;

        // Iterate through all topics in the broker's topic list
        for (String topicId : this.topicList.keySet()) {
            JSONObject topicInfo = new JSONObject();
            topicInfo.put("topic id", topicId);
            topicInfo.put("title", this.topicList.get(topicId));
            topicInfo.put("publisher", this.publisherTopic.get(topicId));
            topicArray.add(topicInfo);
            exist = true;
        }

        // Check if topics exist
        if (exist) {
            response.put("result", "success");
            response.put("detail", topicArray);
        } else {
            response.put("result", "failed");
            response.put("detail", "there is no topic");
        }
        response.put("message type", "list");

        return response;
    }

    /**
     * Subscribes a user to a specific topic if the topic exists.
     * The method also synchronizes the subscription with other brokers in the
     * network.
     *
     * @param topicId    The ID of the topic to subscribe to.
     * @param subscriber The name of the subscriber.
     * @return A JSONObject containing the result of the subscription operation.
     */
    public synchronized JSONObject subscribe(String topicId, String subscriber) {
        JSONObject response = new JSONObject();

        // Check if the subscriber is already subscribed to the topic
        if (!this.subscriberTopic.containsKey(subscriber) || !this.subscriberTopic.get(subscriber).contains(topicId)) {
            if (this.topicList.containsKey(topicId)) {
                this.subscriberTopic.putIfAbsent(subscriber, new HashSet<>());
                this.subscriberTopic.get(subscriber).add(topicId);
                response.put("result", "success");
                response.put("detail", "successfully subscribed to " + topicId);
                syncSubscribeWithOtherBrokers(subscriber, topicId);
            } else {
                response.put("result", "failed");
                response.put("detail", "topic id: " + topicId + " does not exist");
            }
        } else {
            response.put("result", "failed");
            response.put("detail", "you are already subscribed to " + topicId);
        }
        response.put("message type", "response");

        return response;
    }

    /**
     * Synchronizes a subscription action with other brokers.
     *
     * @param subscriber The name of the subscriber.
     * @param topicId    The ID of the topic.
     */
    private synchronized void syncSubscribeWithOtherBrokers(String subscriber, String topicId) {
        JSONObject syncMessage = new JSONObject();
        syncMessage.put("command", "sync");
        syncMessage.put("syncAction", "subscribe");
        syncMessage.put("subscriber", subscriber);
        syncMessage.put("topic id", topicId);

        sendSyncMessageToOtherBrokers(syncMessage);
    }

    /**
     * Unsubscribes a user from a specific topic.
     * The method also synchronizes the unsubscription with other brokers in the
     * network.
     *
     * @param topicId    The ID of the topic to unsubscribe from.
     * @param subscriber The name of the subscriber.
     * @return A JSONObject containing the result of the unsubscription operation.
     */
    public synchronized JSONObject unsubscribe(String topicId, String subscriber) {
        JSONObject response = new JSONObject();
        response.put("message type", "response");

        // Check if the subscriber is subscribed to the topic
        if (this.subscriberTopic.containsKey(subscriber) && this.subscriberTopic.get(subscriber).contains(topicId)) {
            this.subscriberTopic.get(subscriber).remove(topicId);
            response.put("result", "success");
            response.put("detail", "successfully unsubscribed from " + topicId);
            syncUnsubscribeWithOtherBrokers(topicId, subscriber);
        } else {
            response.put("result", "failed");
            response.put("detail", "you are not originally subscribed to " + topicId);
        }
        return response;
    }

    /**
     * Synchronizes an unsubscription action with other brokers.
     *
     * @param subscriber The name of the subscriber.
     * @param topicId    The ID of the topic.
     */
    private synchronized void syncUnsubscribeWithOtherBrokers(String topicId, String subscriber) {
        JSONObject syncMessage = new JSONObject();
        syncMessage.put("command", "sync");
        syncMessage.put("syncAction", "unsubscribe");
        syncMessage.put("subscriber", subscriber);
        syncMessage.put("topic id", topicId);

        sendSyncMessageToOtherBrokers(syncMessage);
    }

    /**
     * Displays the current topics a subscriber is subscribed to, including the
     * topic title and publisher.
     *
     * @param subscriber The name of the subscriber.
     * @return A JSONObject containing the list of topics the subscriber is
     *         currently subscribed to.
     */
    public synchronized JSONObject showCurrentSubscription(String subscriber) {
        JSONObject response = new JSONObject();
        JSONArray subscribedTopics = new JSONArray();
        boolean exist = false;

        // Check if the subscriber is subscribed to any topics
        if (!this.subscriberTopic.isEmpty()) {
            if (this.subscriberTopic.containsKey(subscriber)) {
                for (String topicId : this.subscriberTopic.get(subscriber)) {
                    JSONObject topicInfo = new JSONObject();
                    topicInfo.put("topic id", topicId);
                    topicInfo.put("title", this.topicList.get(topicId));
                    topicInfo.put("publisher", this.publisherTopic.get(topicId));
                    subscribedTopics.add(topicInfo);
                    exist = true;
                }
            }
        }

        // Check if the subscriber is subscribed to any topics
        if (exist) {
            response.put("result", "success");
            response.put("detail", subscribedTopics);
        } else {
            response.put("result", "failed");
            response.put("detail", "you are not subscribed to any topic.");
        }
        response.put("message type", "current");

        return response;
    }

    /**
     * Deletes all topics that a subscriber is subscribed to and synchronizes this
     * action with other brokers.
     *
     * @param subscriber The name of the subscriber.
     */
    public synchronized void deleteAllTopicBySubscriber(String subscriber) {
        subscriberTopic.remove(subscriber); // Remove all topics for the subscriber
        syncDeleteAllTopicsBySubscriberWithOtherBrokers(subscriber);
    }

    /**
     * Synchronizes the deletion of all topics for a specific subscriber with other
     * brokers.
     *
     * @param subscriber The name of the subscriber.
     */
    private synchronized void syncDeleteAllTopicsBySubscriberWithOtherBrokers(String subscriber) {
        JSONObject syncMessage = new JSONObject();
        syncMessage.put("command", "sync");
        syncMessage.put("syncAction", "deleteAllTopicsBySubscriber");
        syncMessage.put("subscriber", subscriber);

        sendSyncMessageToOtherBrokers(syncMessage); // Send sync message to other brokers
    }

    /**
     * Handles synchronization messages between brokers.
     * This method receives a sync message and processes the action accordingly
     * (e.g., create, delete, publish, etc.).
     *
     * @param syncMessage The synchronization message received from another broker,
     *                    as a JSONObject.
     */
    public synchronized void handleSyncMessage(JSONObject syncMessage) {
        String syncAction = (String) syncMessage.get("syncAction");
        String topicId;
        String topicName;
        String publisher;
        String subscriber;

        switch (syncAction) {
            case "create":
                topicId = (String) syncMessage.get("topic id");
                topicName = (String) syncMessage.get("topic name");
                publisher = (String) syncMessage.get("publisher");
                this.syncCreateTopic(topicId, topicName, publisher); // Create topic locally
                break;

            case "delete":
                topicId = (String) syncMessage.get("topic id");
                publisher = (String) syncMessage.get("publisher");
                this.syncDeleteTopic(topicId, publisher); // Delete topic locally
                break;

            case "publish":
                topicId = (String) syncMessage.get("topic id");
                String message = (String) syncMessage.get("message");
                publisher = (String) syncMessage.get("publisher");
                this.syncPublishMessage(topicId, message, publisher); // Publish message locally
                break;

            case "subscribe":
                subscriber = (String) syncMessage.get("subscriber");
                topicId = (String) syncMessage.get("topic id");
                this.syncSubscribe(topicId, subscriber); // Add subscriber locally
                break;

            case "unsubscribe":
                subscriber = (String) syncMessage.get("subscriber");
                topicId = (String) syncMessage.get("topic id");
                this.syncUnsubscribe(subscriber, topicId); // Remove subscriber locally
                break;

            case "deleteAllTopicsByPublisher":
                ArrayList<String> topicsToDelete = (ArrayList<String>) syncMessage.get("deleted topics");
                publisher = (String) syncMessage.get("publisher");
                this.syncDeleteAllTopicByPublisher(publisher, topicsToDelete); // Delete all topics by publisher locally
                break;

            case "deleteAllTopicsBySubscriber":
                subscriber = (String) syncMessage.get("subscriber");
                this.syncDeleteAllTopicBySubscriber(subscriber); // Delete all topics for a subscriber locally
                break;

            default:
                System.out.println("Unknown sync command: " + syncAction);
        }
    }

    /**
     * Sends synchronization messages to other brokers connected to this broker.
     *
     * @param syncMessage The synchronization message to send, as a JSONObject.
     */
    private synchronized void sendSyncMessageToOtherBrokers(JSONObject syncMessage) {

        // Send the sync message to each connected broker
        for (Socket brokerSocket : connectedBrokerSockets) {
            try {
                PrintWriter writer = new PrintWriter(brokerSocket.getOutputStream(), true);
                writer.println(syncMessage.toJSONString()); // Send sync message
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Synchronizes the creation of a topic with other brokers by updating the local
     * broker's state.
     *
     * @param topicId   The ID of the topic being created.
     * @param topicName The name of the topic being created.
     * @param publisher The publisher associated with the topic.
     */
    private synchronized void syncCreateTopic(String topicId, String topicName, String publisher) {
        this.topicList.put(topicId, topicName);
        this.publisherTopic.put(topicId, publisher);
    }

    /**
     * Synchronizes the publishing of a message with other brokers by notifying
     * relevant subscribers locally.
     *
     * @param topicId   The ID of the topic being published to.
     * @param message   The message content.
     * @param publisher The publisher sending the message.
     */
    private synchronized void syncPublishMessage(String topicId, String message, String publisher) {
        ArrayList<String> subscribers = new ArrayList<>();
        String title = this.topicList.get(topicId);

        // Find all subscribers for the topic
        for (HashMap.Entry<String, HashSet<String>> entry : this.subscriberTopic.entrySet()) {
            String subscriber = entry.getKey();
            HashSet<String> idSet = entry.getValue();
            if (idSet.contains(topicId)) {
                subscribers.add(subscriber);
            }
        }

        // Send the message to all subscribers
        if (!subscribers.isEmpty()) {
            for (String subscriber : subscribers) {
                sendMessageToSubscriber(subscriber, message, title, publisher, topicId);
            }
        }
    }

    /**
     * Synchronizes the deletion of a topic with other brokers by updating the local
     * broker's state.
     *
     * @param topicId   The ID of the topic being deleted.
     * @param publisher The publisher associated with the topic.
     */
    private synchronized void syncDeleteTopic(String topicId, String publisher) {
        if (!this.topicList.containsKey(topicId) || !this.publisherTopic.containsKey(topicId) ||
                !this.publisherTopic.get(topicId).equals(publisher)) {
            return; // Exit if the topic or publisher doesn't exist
        }

        String title = this.topicList.get(topicId);

        // Remove topic locally
        this.topicList.remove(topicId);
        this.publisherTopic.remove(topicId);

        // Notify subscribers about the deletion
        for (Map.Entry<String, HashSet<String>> entry : subscriberTopic.entrySet()) {
            HashSet<String> subscribedTopics = entry.getValue();
            if (subscribedTopics.contains(topicId)) {
                String subscriber = entry.getKey();
                subscribedTopics.remove(topicId); // Remove topic from subscriber's list
                notifySubscriber(topicId, subscriber, title, publisher); // Notify subscriber
            }
        }
    }

    /**
     * Synchronizes the deletion of all topics for a publisher by updating the local
     * broker's state.
     *
     * @param publisher  The publisher whose topics are being deleted.
     * @param idToRemove List of topic IDs to be deleted.
     */
    private synchronized void syncDeleteAllTopicByPublisher(String publisher, ArrayList<String> idToRemove) {
        for (HashMap.Entry<String, HashSet<String>> entry : subscriberTopic.entrySet()) {
            JSONObject message = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            String subscriber = "";

            // Create deletion notifications
            for (String topicId : idToRemove) {
                if (entry.getValue().contains(topicId)) {
                    String title = this.topicList.get(topicId);
                    subscriber = entry.getKey();
                    JSONObject topicInfo = new JSONObject();
                    topicInfo.put("topic id", topicId);
                    topicInfo.put("title", title);
                    topicInfo.put("publisher", publisher);
                    jsonArray.add(topicInfo);
                    this.subscriberTopic.get(subscriber).remove(topicId);
                }
            }
            message.put("message type", "deleteNotify");
            message.put("deleted topic", jsonArray);
            if (!subscriber.isEmpty()) {
                notifySubscriber(message, subscriber); // Notify subscribers about the deletion
            }
        }

        // Remove topics locally
        for (String topicId : idToRemove) {
            this.topicList.remove(topicId);
            this.publisherTopic.remove(topicId);
        }
    }

    /**
     * Synchronizes a subscriber's subscription to a topic with other brokers.
     *
     * @param topicId    The ID of the topic the subscriber is subscribing to.
     * @param subscriber The subscriber's name.
     */
    private synchronized void syncSubscribe(String topicId, String subscriber) {
        if (!this.subscriberTopic.containsKey(subscriber) || !this.subscriberTopic.get(subscriber).contains(topicId)) {
            if (this.topicList.containsKey(topicId)) {
                this.subscriberTopic.putIfAbsent(subscriber, new HashSet<>());
                this.subscriberTopic.get(subscriber).add(topicId);
            }
        }
    }

    /**
     * Synchronizes a subscriber's unsubscription from a topic with other brokers.
     *
     * @param topicId    The ID of the topic the subscriber is unsubscribing from.
     * @param subscriber The subscriber's name.
     */
    private synchronized void syncUnsubscribe(String topicId, String subscriber) {
        if (this.subscriberTopic.containsKey(subscriber) && this.subscriberTopic.get(subscriber).contains(topicId)) {
            this.subscriberTopic.get(subscriber).remove(topicId);
        }
    }

    /**
     * Synchronizes the deletion of all topics for a subscriber by updating the
     * local broker's state.
     *
     * @param subscriber The subscriber whose topics are being deleted.
     */
    public synchronized void syncDeleteAllTopicBySubscriber(String subscriber) {
        this.subscriberTopic.remove(subscriber); // Remove all topics for the subscriber
    }

}

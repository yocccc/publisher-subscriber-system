package subscriber;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Represents a subscriber that interacts with a broker in a publisher-subscriber system.
 * The subscriber can view available topics, subscribe to topics, see current subscriptions,
 * and unsubscribe from topics. It also listens for updates from the broker.
 * It can either connect directly to a broker or use the '-d' option to query a Directory Service for available brokers.
 */
public class Subscriber {
    private static final String LIST = "list";
    private static final String SUBSCRIBE = "sub";
    private static final String CURRENT = "current";
    private static final String UNSUBSCRIBE = "unsub";
    private static final String EXIT = "exit";

    private final PrintWriter writer;
    private static final Object lock = new Object();  // Lock object for synchronization

    /**
     * Constructor to initialize the Subscriber with a reader and writer for communication with the broker.
     *
     * @param writer the PrintWriter to send requests to the broker
     */
    public Subscriber(PrintWriter writer) {
        this.writer = writer;
    }

    /**
     * The main method starts the subscriber, connects to the broker, and listens for commands from the user.
     * It also starts a separate thread to listen for incoming messages from the broker.
     *
     * @param args Command-line arguments. First argument is the username.
     *             Use '-d' followed by Directory Service IP and port to query for brokers.
     */
    public static void main(String[] args) {
        String brokerIp;
        int brokerPort;
        // Check if '-d' option is specified to use Directory Service
        if (args.length > 1 && args[1].equals("-d")) {
            String[] directoryAddress = args[2].split(":");
            String directoryIp = directoryAddress[0];
            int directoryPort = Integer.parseInt(directoryAddress[1]);
            JSONArray brokerList = getBrokers(directoryIp, directoryPort);

            if (brokerList.isEmpty()) {
                System.out.println("No available brokers found.");
                return;
            }

            // Randomly select a broker from the list
            Random random = new Random();
            int randomIndex = random.nextInt(brokerList.size());
            JSONObject brokerInfo = (JSONObject) brokerList.get(randomIndex);
            brokerIp = (String) brokerInfo.get("brokerIp");
            brokerPort = Integer.parseInt((String) brokerInfo.get("brokerPort"));
            System.out.println("Connecting to broker: " + brokerIp + ":" + brokerPort);

        } else {
            // Use the broker IP and port specified in the command-line arguments directly to connect to the broker
            String[] address = args[1].split(":");
            brokerIp = address[0];
            brokerPort = Integer.parseInt(address[1]);
            System.out.println("Connecting to broker: " + brokerIp + ":" + brokerPort);
        }

        System.out.println();

        try (Socket socket = new Socket(brokerIp, brokerPort);
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            Subscriber subscriber = new Subscriber(writer);
            String userName=args[0];
            subscriber.sendUserInfo(userName);

            MessageReceiverThread messageReceiverThread = new MessageReceiverThread(socket, lock);
            messageReceiverThread.start();  // Start the message receiving thread

            boolean flag = true;
            Scanner scanner = new Scanner(System.in);
            while (flag) {
                displayMenu();
                String reqString = scanner.nextLine();
                boolean[] results= subscriber.handleCommand(reqString);
                boolean isValidCommand=results[1];
                flag = results[0];
                if (flag && isValidCommand) {
                    subscriber.waitForResponse();// Wait for the broker's response
                }
            }

        } catch (UnknownHostException e) {
            System.out.println("Server not found");
        } catch (IOException e) {
            System.out.println("The server seems to be down. Terminating the program.");
        }
    }

    /**
     * Displays the available commands to the user.
     */
    public static void displayMenu() {
        System.out.println("Please select command: list, sub, current, unsub.");
        System.out.println("1. list #all topics");
        System.out.println("2. sub {topic_id} #subscribe to a topic");
        System.out.println("3. current # show the current subscriptions of the subscriber");
        System.out.println("4. unsub {topic_id} #unsubscribe from a topic");
        System.out.println("5. exit");
    }

    /**
     * Sends the subscriber's user information (user name and type) to the broker.
     *
     * @param userName the name of the subscriber
     */
    private void sendUserInfo(String userName) {
        JSONObject userInfo = new JSONObject();
        userInfo.put("user type", "subscriber");
        userInfo.put("user name", userName);
        this.writer.println(userInfo.toJSONString());
        System.out.println("Connected to the broker");
    }

    /**
     * Retrieves the list of available brokers from the Directory Service.
     *
     * @param directoryIp   The IP address of the Directory Service.
     * @param directoryPort The port number of the Directory Service.
     * @return A JSONArray containing the list of brokers.
     */
    public static JSONArray getBrokers(String directoryIp, int directoryPort) {
        try (Socket directorySocket = new Socket(directoryIp, directoryPort);
             BufferedReader reader = new BufferedReader(new InputStreamReader(directorySocket.getInputStream()));
             PrintWriter writer = new PrintWriter(directorySocket.getOutputStream(), true)) {

            // Send request to Directory Service for the list of brokers
            JSONObject request = new JSONObject();
            request.put("user type", "subscriber");
            writer.println(request.toJSONString());

            // Parse the response from Directory Service
            String response = reader.readLine();
            JSONParser parser = new JSONParser();
            JSONObject res = (JSONObject) parser.parse(response);

            return (JSONArray) res.get("brokers");

        } catch (IOException | ParseException e) {
            e.printStackTrace();
            return new JSONArray(); // Return empty list on failure
        }
    }

    /**
     * Handles the command input from the user and executes the corresponding action.
     *
     * @param reqString the input command string from the user
     * @return true if the program should continue running, false to exit
     */
    private boolean[] handleCommand(String reqString) {
        String[] req = reqString.split(" ");
        String command = req[0];
        boolean[] returnBooleans = new boolean[2];
        returnBooleans[1]=true; //check if command is valid
        returnBooleans[0]=true; // check exit
        switch (command) {
            case LIST:
                sendRequest("list", null);
                break;
            case SUBSCRIBE:
                returnBooleans[1]=handleSubscription(req, "subscribe");
                break;
            case CURRENT:
                sendRequest("showCurrentSubscription", null);
                break;
            case UNSUBSCRIBE:
                returnBooleans[1]=handleSubscription(req, "unsubscribe");
                break;
            case EXIT:
                System.out.println("Program terminated.");
                returnBooleans[0]=false;
                break;
            default:
                System.out.println("Invalid command. Please re-enter.");
                returnBooleans[1]=false;
        }
        return returnBooleans;
    }

    /**
     * Sends a request to the broker with the specified command and topic ID (if applicable).
     *
     * @param command the command to be sent to the broker
     * @param topicId the topic ID (optional, can be null for some commands)
     */
    private void sendRequest(String command, String topicId) {
        JSONObject request = new JSONObject();
        request.put("command", command);
        if (topicId != null) {
            request.put("topic id", topicId);
        }
        this.writer.println(request.toJSONString());
    }

    /**
     * Handles subscription and unsubscription requests from the user.
     *
     * @param req the input command split into an array
     * @param command the command (subscribe or unsubscribe)
     */
    private boolean handleSubscription(String[] req, String command) {
        boolean isValidCommand=false;
        try {
            String topicId = req[1];
            Integer.parseInt(topicId);  // Validate that the topic ID is a number
            sendRequest(command, topicId);
            isValidCommand=true;
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Invalid command. Please re-enter.");
        } catch (NumberFormatException e) {
            System.out.println("ID accepts only number.");
        }
        return  isValidCommand;
    }

    /**
     * Waits for a response from the broker. This method is synchronized with a lock object to handle
     * concurrent access to the socket's input stream.
     */
    private void waitForResponse() {
        synchronized (lock) {
            try {
                lock.wait();  // Wait until notified by the MessageReceiverThread
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // Handle interruption
            }
        }
    }

}

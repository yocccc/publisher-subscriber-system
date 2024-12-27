package publisher;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Represents a publisher that interacts with a broker in a publisher-subscriber
 * system.
 * The publisher can create topics, publish messages to topics, show subscriber
 * counts for their topics, and delete topics.
 * It can either connect directly to a broker or use the '-d' option to query a
 * Directory Service for available brokers.
 */
public class Publisher {
    private static final String CREATE = "create";
    private static final String PUBLISH = "publish";
    private static final String SHOW = "show";
    private static final String DELETE = "delete";
    private static final String EXIT = "exit";

    private BufferedReader reader;
    private PrintWriter writer;

    /**
     * Constructor to initialize the Publisher with a reader and writer for
     * communication with the broker.
     *
     * @param reader the BufferedReader for reading messages from the broker
     * @param writer the PrintWriter for sending messages to the broker
     */
    public Publisher(BufferedReader reader, PrintWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    /**
     * Main method to start the Publisher.
     * The Publisher can either connect directly to a broker or use the Directory
     * Service to find one.
     *
     * @param args Command-line arguments. First argument is the username.
     *             Use '-d' followed by Directory Service IP and port to query for
     *             brokers.
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
                System.out.println("No available brokers");
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
            // Use the broker IP and port specified in the command-line arguments directly
            // to connect to the broker
            String[] address = args[1].split(":");
            brokerIp = address[0];
            brokerPort = Integer.parseInt(address[1]);
            System.out.println("Connecting to specified broker: " + brokerIp + ":" + brokerPort);
        }

        try (Socket socket = new Socket(brokerIp, brokerPort);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            Publisher publisher = new Publisher(reader, writer);
            String userName = args[0];
            publisher.sendUserInfo(userName);
            System.out.println("Connected to the broker");

            boolean flag = true;
            Scanner scanner = new Scanner(System.in);
            while (flag) {
                System.out.println();
                displayMenu();
                System.out.println();
                String reqString = scanner.nextLine();
                flag = publisher.handleCommand(reqString);
            }
        } catch (UnknownHostException ex) {
            System.out.println("Server not found");
        } catch (IOException ex) {
            System.out.println("The server seems to be down. Terminating the program.");
        } catch (ParseException e) {
            System.out.println("Error parsing server response. Please try again.");
        }
    }

    /**
     * Displays the menu of available commands for the publisher.
     */
    private static void displayMenu() {
        System.out.println("Please select command: create, publish, show, delete.");
        System.out.println("1. create {topic_id} {topic_name} #create a new topic");
        System.out.println("2. publish {topic_id} {message} # publish a message to an existing topic");
        System.out.println("3. show  #show subscriber count for current publisher");
        System.out.println("4. delete {topic_id} #delete a topic");
        System.out.println("5. exit");
    }

    /**
     * Sends user information (user name and type) to the broker when establishing a
     * connection.
     *
     * @param userName the name of the user (publisher)
     */
    private void sendUserInfo(String userName) {
        JSONObject userInfo = new JSONObject();
        userInfo.put("user type", "publisher");
        userInfo.put("user name", userName);
        this.writer.println(userInfo.toJSONString());
    }

    /**
     * Handles the command input from the user and executes the appropriate action.
     *
     * @param reqString the input command string from the user
     * @return true if the program should continue running, false to exit
     * @throws IOException    if there is an error in communication with the broker
     * @throws ParseException if there is an error parsing the broker's response
     */
    private boolean handleCommand(String reqString) throws IOException, ParseException {
        String[] req = reqString.split(" ");
        String command = req[0];

        switch (command) {
            case CREATE:
                create(req);
                break;
            case PUBLISH:
                publish(req);
                break;
            case SHOW:
                show();
                break;
            case DELETE:
                delete(req);
                break;
            case EXIT:
                System.out.println("Program terminated.");
                return false;
            default:
                System.out.println("Invalid command. Please re-enter.");
        }
        return true;
    }

    /**
     * Retrieves the list of available brokers from the Directory Service.
     *
     * @param directoryIp   The IP address of the Directory Service.
     * @param directoryPort The port number of the Directory Service.
     * @return A JSONArray containing the list of brokers.
     */
    public static JSONArray getBrokers(String directoryIp, int directoryPort) {
        try (Socket socket = new Socket(directoryIp, directoryPort);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            JSONObject request = new JSONObject();
            request.put("user type", "publisher");
            writer.println(request.toJSONString());

            String response = reader.readLine();
            JSONParser parser = new JSONParser();
            JSONObject res = (JSONObject) parser.parse(response);

            return (JSONArray) res.get("brokers");

        } catch (IOException | ParseException e) {
            System.out.println("Failed to retrieve the list of available broker");
            return new JSONArray();
        }
    }

    /**
     * Handles the creation of a new topic.
     *
     * @param req the command input containing the topic ID and name
     * @throws IOException    if there is an error in communication with the broker
     * @throws ParseException if there is an error parsing the broker's response
     */
    private void create(String[] req) throws IOException, ParseException {
        if (req.length < 3) {
            System.out.println("Invalid command. Please provide topic id and name.");
            return;
        }

        String topicId = req[1];
        String topicName = String.join(" ", Arrays.copyOfRange(req, 2, req.length));
        ;

        try {
            Integer.parseInt(topicId);
        } catch (NumberFormatException e) {
            System.out.println("Topic id must be a number.");
            return;
        }

        JSONObject request = new JSONObject();
        request.put("command", "create");
        request.put("topic id", topicId);
        request.put("topic name", topicName);
        this.writer.println(request.toJSONString());

        String line = this.reader.readLine();
        JSONParser parser = new JSONParser();
        JSONObject res = (JSONObject) parser.parse(line);

        System.out.println(res.get("detail"));
    }

    /**
     * Handles the publishing of a message to an existing topic.
     *
     * @param req the command input containing the topic ID and the message
     * @throws IOException    if there is an error in communication with the broker
     * @throws ParseException if there is an error parsing the broker's response
     */
    private void publish(String[] req) throws IOException, ParseException {
        if (req.length < 3) {
            System.out.println("Invalid command. Please provide topic id and message.");
            return;
        }

        String topicId = req[1];
        String message = String.join(" ", Arrays.copyOfRange(req, 2, req.length));

        try {
            Integer.parseInt(topicId);
        } catch (NumberFormatException e) {
            System.out.println("Topic id must be a number.");
            return;
        }

        if (message.length() > 100) {
            System.out.println("Message exceeds the maximum length of 100 characters.");
            return;
        }

        JSONObject request = new JSONObject();
        request.put("command", "publish");
        request.put("topic id", topicId);
        request.put("message", message);
        this.writer.println(request.toJSONString());

        String line = this.reader.readLine();
        JSONParser parser = new JSONParser();
        JSONObject res = (JSONObject) parser.parse(line);

        System.out.println(res.get("detail"));
    }

    /**
     * Requests and displays the subscriber count for topics owned by the current
     * publisher.
     *
     * @throws IOException    if there is an error in communication with the broker
     * @throws ParseException if there is an error parsing the broker's response
     */
    private void show() throws IOException, ParseException {
        JSONObject request = new JSONObject();
        request.put("command", "countSubscriber");
        this.writer.println(request.toJSONString());

        String line = this.reader.readLine();
        JSONParser parser = new JSONParser();
        JSONObject res = (JSONObject) parser.parse(line);

        if ("success".equals(res.get("result"))) {
            JSONArray countList = (JSONArray) res.get("detail");
            for (Object item : countList) {
                JSONObject jsonObject = (JSONObject) item;
                String title = (String) jsonObject.get("title");
                String topicId = (String) jsonObject.get("topic id");
                String count = (String) jsonObject.get("count");
                System.out.println(topicId + " " + title + " " + count);
            }
        } else {
            System.out.println((String) res.get("detail"));
        }
    }

    /**
     * Handles the deletion of an existing topic.
     *
     * @param req the command input containing the topic ID to be deleted
     * @throws IOException    if there is an error in communication with the broker
     * @throws ParseException if there is an error parsing the broker's response
     */
    private void delete(String[] req) throws IOException, ParseException {
        if (req.length < 2) {
            System.out.println("Invalid command. Please provide topic id.");
            return;
        }

        String topicId = req[1];

        try {
            Integer.parseInt(topicId);
        } catch (NumberFormatException e) {
            System.out.println("Topic id must be a number.");
            return;
        }

        JSONObject request = new JSONObject();
        request.put("command", "delete");
        request.put("topic id", topicId);
        this.writer.println(request.toJSONString());

        String line = this.reader.readLine();
        JSONParser parser = new JSONParser();
        JSONObject res = (JSONObject) parser.parse(line);

        System.out.println(res.get("detail"));
    }

}

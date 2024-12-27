/** @author Yoshikazu Fujisaka
 */

package subscriber;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * MessageReceiverThread is responsible for continuously listening for messages
 * from the broker.
 * It handles broadcast messages, topic deletion notifications, and general
 * responses,
 * and synchronizes with the main thread to notify once a message has been
 * processed.
 */
class MessageReceiverThread extends Thread {
    private Socket socket;
    private final Object lock;

    /**
     * Constructor to initialize the message receiver thread with the socket, and
     * lock object.
     *
     * @param socket the Socket connected to the broker
     * @param lock   the object used for synchronizing with the main thread
     */
    public MessageReceiverThread(Socket socket, Object lock) {
        this.socket = socket;
        this.lock = lock;
    }

    /**
     * Continuously listens for incoming messages from the broker.
     * It processes the message based on its type and notifies the main thread after
     * processing.
     */
    @Override
    public void run() {
        String line;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while ((line = reader.readLine()) != null) {
                System.out.println();
                JSONParser parser = new JSONParser();
                JSONObject res = (JSONObject) parser.parse(line);
                String messageType = (String) res.get("message type");
                if ("broadcast".equals(messageType)) {
                    handleBroadcast(res);
                } else if ("deleteNotify".equals(messageType)) {
                    handleDeleteNotify(res);
                } else {
                    handleGeneralResponse(res);
                }
                // Notify the main thread once a message is processed
                synchronized (lock) {
                    lock.notify();
                }
                System.out.println();
            }
        } catch (IOException e) {
            if (!socket.isClosed()) {
                System.out.println("Server down.");
            }
        } catch (ParseException e) {
            System.out.println("Failed to parse.");
        }
    }

    /**
     * Handles broadcast messages from the broker, printing out details about the
     * message received.
     *
     * @param res the JSONObject containing the broadcast message details
     */
    private void handleBroadcast(JSONObject res) {
        String publisher = (String) res.get("publisher");
        String title = (String) res.get("title");
        String topicId = (String) res.get("topic id");
        String message = (String) res.get("message");

        System.out.println("\nYou have received a message");
        System.out.println("Publisher: " + publisher + " | Topic ID: " + topicId + " | Title: " + title
                + " | Message: \"" + message + "\"");
        System.out.println();
        Subscriber.displayMenu();
    }

    /**
     * Handles delete notifications from the broker, listing the topics that have
     * been deleted.
     *
     * @param res the JSONObject containing the delete notification details
     */
    private void handleDeleteNotify(JSONObject res) {
        System.out.println("Deleted topics:");
        JSONArray deletedTopics = (JSONArray) res.get("deleted topic");
        for (Object item : deletedTopics) {
            JSONObject jsonObject = (JSONObject) item;
            String topicId = (String) jsonObject.get("topic id");
            String title = (String) jsonObject.get("title");
            String publisher = (String) jsonObject.get("publisher");
            System.out.println("ID " + topicId + ": " + title + " from " + publisher + " was deleted.");
        }
        System.out.println();
        Subscriber.displayMenu();
    }

    /**
     * Handles general responses from the broker, including topic listings or
     * response details.
     *
     * @param res the JSONObject containing the response details
     */
    private void handleGeneralResponse(JSONObject res) {
        Object detail = res.get("detail");
        String messageType = res.get("message type").toString();

        if (detail instanceof JSONArray) {
            if (messageType.equals("current")) {
                System.out.println("Subscribed Topics:");
            } else if (messageType.equals("list")) {
                System.out.println("Available Topics:");
            }
            for (Object item : (JSONArray) detail) {
                JSONObject jsonObject = (JSONObject) item;
                String topicId = (String) jsonObject.get("topic id");
                String title = (String) jsonObject.get("title");
                String publisher = (String) jsonObject.get("publisher");
                System.out.println("Topic ID: " + topicId + " | Title: " + title + " | Publisher: " + publisher);
            }
        } else {
            System.out.println((String) res.get("detail"));
        }
    }
}

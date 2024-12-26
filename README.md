# README

## 概要
publisher-subscriber型の分散システムです。任意の数のpublisher、subscriberは起動するとdirectory serverに接続し、directory serverによって割り振られた任意の数のbrokerの内1つに接続します。
broker同士リアルタイムで情報を共有しており、publisherが接続されているbrokerに新たにトピックを発行したりメッセージを送信すると、それを購読しているsubscriber全員に届きます。

## USE
This system supports two connection methods: using a directory service or specifying the connection destination directly. You can switch between these methods using the `-d` and `-b` options.

### Example of Execution Using the Directory Service (`-d` option)
```
bash
# Start the directory service
java -jar directory.jar 9999

# Start the broker and register it with the directory service
java -jar broker.jar 6666 -d localhost:9999

# Start the publisher and connect to a broker using the directory service
java -jar publisher.jar pub1 -d localhost:9999

# Start the subscriber and connect to a broker using the directory service
java -jar subscriber.jar sub1 -d localhost:9999


## Example of Execution Without the Directory Service (Using -b option)

# Start a broker without the directory service
java -jar broker.jar 8080

# Start another broker and connect it to the first broker
java -jar broker.jar  7777 -b localhost:8080

# Start another broker and connect it to the first and second broker
java -jar broker.jar 6666 -b localhost:8080 localhost:7777

# Start the publisher and connect it directly to a broker
java -jar publisher.jar pub1 localhost:8080

# Start the subscriber and connect it directly to another broker
java -jar subscriber.jar sub1 localhost:7777
```

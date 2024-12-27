# README

## 概要
publisher-subscriber型の分散システムです。任意の数のpublisher、subscriberは起動するとdirectory serverに接続し、directory serverによって割り振られた任意の数のbrokerの内1つに接続します。
broker同士リアルタイムで情報を共有しており、publisherが接続されているbrokerに新たにトピックを発行したりメッセージを送信すると、それを購読しているsubscriber全員に届きます。

# 分散型パブリッシャー-サブスクライバーシステム
分散型のパブリッシャー-サブスクライバー（Pub-Sub）システムを設計・実装しました。このシステムは、複数のパブリッシャーとサブスクライバーがブローカーに同時にアクセスすることを効果的に管理し、トピックのクエリ、サブスクライブ、削除、追加、公開といった操作を可能にし、ブローカー間で情報の一貫性を維持します。
## 特徴

- **同期とネットワーク処理**: 並行処理を活用してパフォーマンスを最適化。
- **スケーラビリティ**: ディレクトリサービスにより、動的接続と負荷分散が可能。
- **情報の一貫性**: ブローカー間の同期により、システムの整合性を維持。

---


## システム構成

### 1. ブローカー

#### 役割
ブローカーは、パブリッシャーとサブスクライバー間の通信を仲介する中心的なコンポーネントです。トピックの管理やメッセージ配信、他のブローカーとの同期を行います。

#### 責任
- トピックの作成および削除を管理。
- パブリッシャーおよびサブスクライバーとの通信を処理。
- 他のブローカーとトピックやメッセージを同期。
- サブスクライバーおよびパブリッシャーのマッピングを維持。

#### 主なクラス
- `Broker`: ブローカーの主要機能を管理。
- `ClientHandler`: パブリッシャー、サブスクライバー、または他のブローカーとの接続を処理。

---

### 2. ディレクトリサービス

#### 役割
ディレクトリサービスは、アクティブなブローカーのリストを管理し、パブリッシャーやサブスクライバーが動的にブローカーを見つけられるようにします。

#### 責任
- ブローカーを登録し、そのIPアドレスとポートを追跡。
- パブリッシャーやサブスクライバーがリクエストした際にアクティブなブローカーのリストを提供。

#### 主なクラス
- `DirectoryService`: ブローカーのリストを管理し、クライアントにブローカーの詳細を提供。
- `DirectoryHandler`: ディレクトリサービスへの接続を処理。

---

### 3. パブリッシャー

#### 役割
パブリッシャーは、トピックを作成し、それらにメッセージを公開します。ブローカーに直接接続するか、ディレクトリサービスを使用して利用可能なブローカーを見つけます。

#### 責任
- ブローカー内でトピックを作成。
- トピックにメッセージを公開。
- 作成したトピックのサブスクライバー数を表示。
- トピックを削除。

#### 主なクラス
- `Publisher`: トピックの作成、メッセージの公開、トピックの管理機能を実装。

---

### 4. サブスクライバー

#### 役割
サブスクライバーは、トピックにサブスクライブして、公開されたメッセージを受け取ります。トピックの確認やサブスクリプションの管理を行います。

#### 責任
- トピックにサブスクライブ。
- サブスクリプションを解除。
- 現在サブスクライブしているトピックを確認。
- メッセージやトピック削除通知を受信。

#### 主なクラス
- `Subscriber`: サブスクリプションおよびブローカーとのやり取りを管理。
- `MessageReceiverThread`: メッセージを受信し、処理を行う。

---

## ユーザーインターフェース

#### 役割
パブリッシャーとサブスクライバーは、コマンドラインインターフェース（CLI）を使用してシステムと通信します。

#### 責任
- トピックの作成、公開、サブスクライブなどのコマンドを実行。
---

## プロトコル

通信は、JSONベースのメッセージングプロトコルを使用して処理されます。

### 主なフィールド
- `user type`: エンティティの種類（ブローカー、パブリッシャー、サブスクライバー）を指定。
- `message type`: メッセージの種類（ブロードキャスト、レスポンス、同期リクエストなど）。
- `user name`: ユーザーの一意な識別子。
- `command`: リクエストされている特定のアクションを指定。
---

## 使い方

### システムの接続方法
このシステムは、以下の2つの接続方法をサポートしています。

1. **ディレクトリサービスを使用する方法** (`-d`オプション)
2. **接続先を直接指定する方法** (`-b`オプション)

#### ディレクトリサービスを使用した実行例 (`-d`オプション)
##### 手順
1. **ディレクトリサービスの起動**
   ```bash
   java -jar directory.jar 起動するポート番号
   java -jar directory.jar 9999
   ```

2. **ブローカーの起動とディレクトリサービスへの登録**
   ```bash
   java -jar broker.jar 起動するポート番号 -d ディレクトリサービスのアドレス
   java -jar broker.jar 6666 -d localhost:9999
   ```

3. **パブリッシャーの起動とディレクトリサービスを介したブローカーへの接続**
   ```bash
   java -jar publisher.jar 名前 -d ディレクトリサービスのアドレス
   java -jar publisher.jar pub1 -d localhost:9999
   ```

4. **サブスクライバーの起動とディレクトリサービスを介したブローカーへの接続**
   ```bash
   java -jar subscriber.jar 名前 -d ディレクトリサービスのアドレス
   java -jar subscriber.jar sub1 -d localhost:9999
   ```

---

#### ディレクトリサービスを使用しない実行例 (`-b`オプション)
##### 手順
1. **ディレクトリサービスを使用せずにブローカーを起動**
   ```bash
   java -jar broker.jar 起動するポート番号
   java -jar broker.jar 8080
   ```

2. **別のブローカーを起動し、最初のブローカーに接続**
   ```bash
   java -jar broker.jar 起動するポート番号 -b ブローカーのアドレス
   java -jar broker.jar 7777 -b localhost:8080
   ```

3. **さらに別のブローカーを起動し、最初と2番目のブローカーに接続**
   ```bash
   java -jar broker.jar 起動するポート番号 -b ブローカーのアドレス1 ブローカーのアドレス2
   java -jar broker.jar 6666 -b localhost:8080 localhost:7777
   ```

4. **パブリッシャーを起動し、任意のブローカーに直接接続**
   ```bash
   java -jar publisher.jar 名前 ブローカーのアドレス
   java -jar publisher.jar pub1 localhost:8080
   ```

5. **サブスクライバーを起動し、任意のブローカーに直接接続**
   ```bash
   java -jar subscriber.jar 名前 ブローカーのアドレス
   java -jar subscriber.jar sub1 localhost:7777
   
---

### サブスクライバーのコマンド
サブスクライバーは以下のコマンドを使用して操作を行います。

- **コマンド選択**
  `Please select command: list, sub, current, unsub.`

#### コマンド一覧
1. **list**  
   - 使用例: `list`
   - 説明: 利用可能な全てのトピックの一覧を取得します。
   - 結果: トピックID、トピック名、パブリッシャー名を含むリストが表示されます。

2. **sub {topic_id}**
   - 使用例: `sub 1234`
   - 説明: 特定のトピック（topic_id）をサブスクライブします。
   - 結果: 対象トピックの今後のすべてのメッセージを受信します。

3. **current**
   - 使用例: `current`
   - 説明: 現在サブスクライブ中のトピックを表示します。
   - 結果: トピックID、トピック名、パブリッシャー名が一覧表示されます。

4. **unsub {topic_id}**
   - 使用例: `unsub 1234`
   - 説明: 指定したトピックのサブスクリプションを解除します。
   - 結果: 対象トピックからのメッセージ受信が停止します。解除確認メッセージが送信されます。

5. **exit**
   - 使用例: `exit`
   - 説明: サブスクライバーを終了します。

---

### パブリッシャーのコマンド
パブリッシャーは以下のコマンドを使用して操作を行います。

#### コマンド一覧
1. **create {topic_id} {topic_name}**
   - 使用例: `create 1234 TopicName`
   - 説明: 新しいトピックを作成します。
     - トピックIDは一意でなければなりません。
     - トピック名は一意でなくても構いません（他のパブリッシャーが同じ名前を使用する場合があります）。

2. **publish {topic_id} {message}**
   - 使用例: `publish 1234 HelloWorld`
   - 説明: 指定したトピックIDに対してメッセージを公開します。
     - メッセージは最大100文字まで許容されます。
     - ブローカーを通じてトピックのサブスクライバー全員に配信されます。

3. **show**
   - 使用例: `show`
   - 説明: 現在のパブリッシャーが所有する各トピックのサブスクライバー数を表示します。

4. **delete {topic_id}**
   - 使用例: `delete 1234`
   - 説明: 指定したトピックを削除します。
     - トピックにサブスクライブしている全てのサブスクライバーが自動的に解除されます。
     - 各サブスクライバーに通知メッセージが送信されます。

5. **exit**
   - 使用例: `exit`
   - 説明: パブリッシャーを終了します。


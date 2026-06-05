# Thingshub Client SDK
Thingshub Client SDK可以让业务系统开发者轻松连接Thingshub Server与设备进行交互。

## 前提条件

* JDK 17+
* Maven

## 安装

从Github仓库check out代码后，使用以下命令构建：
```
mvn clean install -DskipTests
```

在项目中添加依赖：

```
<dependency>
    <groupId>io.thingshub</groupId>
	<artifactId>thingshub-client</artifactId>
    <version>${thingshub.client.version}</version>
</dependency>
```

## 使用示例

### 配置Thingshub Server连接信息
```java
thingshub: 
  host: 10.40.174.195
  port: 1883
  username: biz_client
  password: 12345678
```

### 发送消息
以下代码表示给产品编号为HY-001、设备序列号为320027880006的设备发送系统升级的命令：
```java
Map<String, Object> updateParams = new HashMap<>();
updateParams.put("url", "http://www.test.com/qlock-app-v4.0.bin");
thingshubClient.publish("HY-001", "320027880006", "update", updateParams);//消息名update和消息参数url需要在Thingshub平台中定义，并授权给当前客户端
```

### 监听设备消息
想要监听设备上报的消息，需要实现MessageProcessor接口。

监听设备回复的消息：
```java
@Component
@Slf4j
public class UpdateAckMessageProcessor implements MessageProcessor<Void> {

	@Override
	public String getMessageName() {
		return "update_ack"; //消息名update_ack需要在Thingshub平台中定义，并授权给当前客户端
	}

	@Override
	public void process(String sn, String messageId, Void payload) {
		log.info("upgrade device({}) successfully", sn);
	}

	@Override
	public void onError(String sn, Integer code, String error) {
		log.error("upgrade device({}) error: {}", sn, error);
	}

}
```

监听设备主动上报的消息：
```java
@Component
@Slf4j
public class StatusMessageProcesser implements MessageProcessor<StatusMessage> {

	@Resource
	private ThingshubClient thingshubClient;

	public static final String REPLY_MESSAGE_NAME = "status_ack";

	@Override
	public String getMessageName() {
		return "status";
	}

	@Override
	public void process(String sn, String messageId, StatusMessage payload) {
		// TODO process device status data

		log.info("Processing Status Data====================");

		//如果通信协议中规定需要回复消息
		thingshubClient.reply("HY-001", sn, REPLY_MESSAGE_NAME, messageId, null);

//		thingshubClient.replyWithError("HY-001", sn, REPLY_MESSAGE_NAME, messageId, 500, "error message");

	}

}
```

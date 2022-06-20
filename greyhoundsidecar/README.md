# Greyhound Sidecar

Sidecar app wraps [Greyhound Open Source](https://github.com/wix/greyhound#readme).<br/>
It can communicate with various client SDKs, and gives easy access to produce and consume messages over kafka.

> 📝 Note:  
> The open source version of Greyhound is still in the initial rollout stage, so the APIs might not be fully stable yet.

## Available Client SDKs

- Python
- JavaScript
- .Net
- More APIs coming soon...

## Sidecar APIs

- **Create Topics**

  Create Kafka Topics without any boilerplate - simply pass the topic name. Multiple topics can be created in a single API call 

- **Register Endpoint**

  In order to be able to receive notifications to consume, Sidecar needs to know where to send consumed messages to. Pass gRPC host and port
```
message RegisterRequest {
 string host = 1;
 string port = 2;
}

```
  
- **Produce Messages**

```
message ProduceRequest {
  string topic = 1;
  google.protobuf.StringValue payload = 2;
  oneof Target {
    string key = 3;
  }
  map<string, string> custom_headers = 4;
}
```
  

- **Consume Messages**

```
message StartConsumingRequest {
  repeated Consumer consumers = 2;
}

message Consumer {
  string id = 1;
  string group = 2;
  string topic = 3;
}
```

## Sidecar User API

- **Handle Messages**

```
message HandleMessagesRequest {
  string group = 1;
  string topic = 2;
  repeated Record records = 3;
}

message Record {
  int32 partition = 1;
  int64 offset = 2;
  google.protobuf.StringValue payload = 3;
  map<string, google.protobuf.StringValue> headers = 4;
  google.protobuf.StringValue key = 5;
}
```

# Greyhound Sidecar Architecture 

![Greyhound](../docs/greyhound-sidecar-oss.png)

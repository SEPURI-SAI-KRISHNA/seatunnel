import ChangeLog from '../changelog/connector-jms.md';

# Jms

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

> 通用 JMS 队列数据汇连接器

## 描述

使用消息代理自身的客户端库，把每一行作为一条消息发送到任何提供 JMS 1.1（`javax.jms`）客户端的消息代理的一个队列。例如 ActiveMQ Artemis、ActiveMQ Classic、IBM MQ、TIBCO EMS 和 Solace；AMQP 1.0 代理通过 Apache Qpid JMS 客户端访问。

本连接器不替代 [ActiveMQ](Activemq.md) 或 [RabbitMQ](Rabbitmq.md) 数据汇，二者保留各自的特有功能。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 安装 JMS 客户端

请把代理的客户端 jar 放到每个运行 SeaTunnel 的节点的 `${SEATUNNEL_HOME}/plugins/connector-jms/` 目录中。数据汇使用与数据源相同的客户端；每个已测试客户端所需的 jar 列在 [Jms 数据源](../source/Jms.md)文档的“安装 JMS 客户端”一节中。

## 配置项

| 名称 | 类型 | 必填 | 默认值 |
| --- | --- | --- | --- |
| connection_factory_class | string | 是，或使用 `jndi_connection_factory_name` | - |
| connection_factory_properties | map | 否 | - |
| jndi_connection_factory_name | string | 是，或使用 `connection_factory_class` | - |
| jndi_initial_context_factory | string | 与 `jndi_connection_factory_name` 一起必填 | - |
| jndi_provider_url | string | 否 | - |
| jndi_properties | map | 否 | - |
| username | string | 否 | - |
| password | string | 否 | - |
| destination | string | 是 | - |
| format | enum | 否 | JSON |
| field_delimiter | string | 否 | , |
| message_type | enum | 否 | TEXT |
| delivery_mode | enum | 否 | PERSISTENT |
| priority | int | 否 | 4 |
| time_to_live | long | 否 | 0 |
| max_uncommitted_messages | int | 否 | 1000 |

连接配置项（`connection_factory_class`、`connection_factory_properties`、`jndi_*` 配置项、`username` 和 `password`）与 [Jms 数据源](../source/Jms.md)中的用法相同。

### destination [string]

队列名称，传给 `Session.createQueue`。其语法由提供方定义。不支持主题（topic）。

### format / field_delimiter

行转换为消息体的方式。`JSON` 写出每列一个字段的 JSON 对象。`TEXT` 用 `field_delimiter`（默认 `,`）连接各列的值。

### message_type [enum]

`TEXT` 发送 `TextMessage`。`BYTES` 发送 `BytesMessage`，其内容为消息体的 UTF-8 字节。

### delivery_mode [enum]

`PERSISTENT`（默认）或 `NON_PERSISTENT`。非持久消息在代理重启时可能丢失。

### priority [int]

消息的 JMS 优先级，从 0（最低）到 9（最高）。JMS 默认值为 4。

### time_to_live [long]

代理保留消息的时间，单位为毫秒。默认值 0 表示消息永不过期。

### max_uncommitted_messages [int]

一个写入器在一个事务中发送的最大消息数。达到该数量时写入器提交事务，并且每个检查点之前也会提交。请使其低于代理对单个事务中未提交消息数的限制，例如 IBM MQ 队列管理器属性 `MaxUncommittedMsgs`。

### 公共配置

参见[数据汇公共配置](../common-options/sink-common-options.md)。

## 交付语义

- 每个写入器在一个事务会话中发送消息。会话提交后消息才可见：达到 `max_uncommitted_messages` 时，以及在 Zeta 和 Flink 上每个检查点之前、批作业结束时、Spark 任务提交时。
- 语义为**至少一次**。写入器失败时，其未提交的消息被丢弃，引擎会重新写入最后一个检查点之后的行，因此已经提交的消息可能被发送两次。重复会造成影响时请使用幂等的消费方。
- 每个并行写入器有自己的连接。同一个写入器内保持消息顺序，不同写入器之间不保证顺序。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}

sink {
  Jms {
    connection_factory_class = "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory"
    connection_factory_properties {
      brokerURL = "tcp://artemis-host:61616"
    }
    username = "seatunnel"
    password = "******"
    destination = "orders"
    message_type = BYTES
    delivery_mode = PERSISTENT
  }
}
```

## 变更日志

<ChangeLog />

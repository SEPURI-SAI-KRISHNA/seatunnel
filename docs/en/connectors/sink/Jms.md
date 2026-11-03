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

> Generic JMS queue sink connector

## Description

Sends each row as one message to one queue of any message broker that has a JMS 1.1 (`javax.jms`) client, using that broker's own client library. Examples are ActiveMQ Artemis, ActiveMQ Classic, IBM MQ, TIBCO EMS and Solace; AMQP 1.0 brokers are reached through the Apache Qpid JMS client.

This connector does not replace the [ActiveMQ](Activemq.md) or [RabbitMQ](Rabbitmq.md) sinks, which keep their provider-specific features.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Install the JMS client

Put the client jars of your broker in `${SEATUNNEL_HOME}/plugins/connector-jms/` on every node that runs SeaTunnel. The sink uses the same clients as the source; the jars for each tested client are listed in the [Jms source](../source/Jms.md) documentation under "Install the JMS client".

## Options

| Name | Type | Required | Default |
| --- | --- | --- | --- |
| connection_factory_class | string | yes, or `jndi_connection_factory_name` | - |
| connection_factory_properties | map | no | - |
| jndi_connection_factory_name | string | yes, or `connection_factory_class` | - |
| jndi_initial_context_factory | string | with `jndi_connection_factory_name` | - |
| jndi_provider_url | string | no | - |
| jndi_properties | map | no | - |
| username | string | no | - |
| password | string | no | - |
| destination | string | yes | - |
| format | enum | no | JSON |
| field_delimiter | string | no | , |
| message_type | enum | no | TEXT |
| delivery_mode | enum | no | PERSISTENT |
| priority | int | no | 4 |
| time_to_live | long | no | 0 |
| max_uncommitted_messages | int | no | 1000 |

The connection options (`connection_factory_class`, `connection_factory_properties`, the `jndi_*` options, `username` and `password`) work as in the [Jms source](../source/Jms.md).

### destination [string]

The queue name, passed to `Session.createQueue`. Its syntax is defined by the provider. Topics are not supported.

### format / field_delimiter

How a row becomes the message body. `JSON` writes a JSON object with one field per column. `TEXT` joins the column values with `field_delimiter` (default `,`).

### message_type [enum]

`TEXT` sends a `TextMessage`. `BYTES` sends a `BytesMessage` holding the body as UTF-8 bytes.

### delivery_mode [enum]

`PERSISTENT` (the default) or `NON_PERSISTENT`. Non-persistent messages can be lost if the broker restarts.

### priority [int]

The JMS priority of the messages, from 0 (lowest) to 9 (highest). The JMS default is 4.

### time_to_live [long]

How long the broker keeps a message, in milliseconds. 0, the default, means the messages never expire.

### max_uncommitted_messages [int]

The maximum number of messages a writer sends in one transaction. The writer commits when it reaches this number, and also before every checkpoint. Keep it below any broker limit on uncommitted messages in one transaction, such as the IBM MQ queue manager attribute `MaxUncommittedMsgs`.

### Common Options

See [Sink Common Options](../common-options/sink-common-options.md).

## Delivery

- Each writer sends in a transacted session. Messages become visible when the session is committed: when `max_uncommitted_messages` is reached, and before every checkpoint on Zeta and Flink, at the end of a batch job, and when a Spark task commits.
- Delivery is **at least once**. If a writer fails, its uncommitted messages are discarded and the engine writes the rows after the last checkpoint again, so messages that were already committed can be sent twice. Use idempotent consumers where duplicates matter.
- Each parallel writer has its own connection. Message order is kept within one writer, not across writers.

## Example

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

## Changelog

<ChangeLog />

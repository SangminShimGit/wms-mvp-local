import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";

export const SHOPIFY_ORDERS_TOPIC = "shopify.orders.created";
export const WMS_INVENTORY_CHANGES_TOPIC = "wms.inventory.changes";

const broker = process.env.KAFKA_BROKER ?? "localhost:9092";
const clientId = process.env.KAFKA_CLIENT_ID ?? "wms-mvp-local";

/**
 * Creates a configured Kafka client instance.
 *
 * @returns Kafka client bound to the configured broker.
 */
export function createKafkaClient(): Kafka {
  return new Kafka({
    clientId,
    brokers: [broker],
    logLevel: logLevel.INFO,
  });
}

/**
 * Creates and connects a Kafka producer.
 *
 * @returns Connected Kafka producer instance.
 * @throws When the producer fails to connect.
 */
export async function createKafkaProducer(): Promise<Producer> {
  const producer = createKafkaClient().producer();
  await producer.connect();
  return producer;
}

/**
 * Creates and connects a Kafka consumer for the Shopify orders topic group.
 *
 * @param groupId Consumer group identifier.
 * @returns Connected Kafka consumer subscribed to Shopify orders topic.
 * @throws When the consumer fails to connect or subscribe.
 */
export async function createKafkaConsumer(
  groupId = "wms-order-consumer"
): Promise<Consumer> {
  const consumer = createKafkaClient().consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({
    topic: SHOPIFY_ORDERS_TOPIC,
    fromBeginning: false,
  });
  return consumer;
}

/**
 * Creates and connects a Kafka consumer for the WMS inventory changes topic.
 *
 * @param groupId Consumer group identifier.
 * @returns Connected Kafka consumer subscribed to inventory changes topic.
 * @throws When the consumer fails to connect or subscribe.
 */
export async function createInventoryChangeConsumer(
  groupId = "wms-inventory-consumer"
): Promise<Consumer> {
  const consumer = createKafkaClient().consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({
    topic: WMS_INVENTORY_CHANGES_TOPIC,
    fromBeginning: false,
  });
  return consumer;
}

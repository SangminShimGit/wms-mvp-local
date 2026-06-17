import "dotenv/config";
import type { Consumer, Producer } from "kafkajs";
import type { Worker } from "bullmq";
import type { Server } from "http";
import {
  createInventoryChangeConsumer,
  createKafkaConsumer,
  createKafkaProducer,
} from "./config/kafka";
import { prisma } from "./config/prisma";
import { createServer, startServer } from "./api/server";
import {
  inventoryChangeQueue,
  startInventoryChangeWorker,
  type InventoryChangePayload,
} from "./workers/bullmq/inventoryChangeWorker";
import {
  startWmsEtlWorker,
  type ShopifyOrderPayload,
  wmsEtlQueue,
} from "./workers/bullmq/worker";
import { inngest } from "./workers/inngest/client";

let producer: Producer | undefined;
let consumer: Consumer | undefined;
let inventoryChangeConsumer: Consumer | undefined;
let worker: Worker<ShopifyOrderPayload> | undefined;
let inventoryChangeWorker:
  | Worker<InventoryChangePayload, { sku: string; stock: number; critical: boolean }>
  | undefined;
let httpServer: Server | undefined;

/**
 * Starts the Kafka consumer and dispatches messages to BullMQ and Inngest.
 *
 * @returns Connected Kafka consumer instance.
 * @throws When consumer startup fails.
 */
async function startKafkaConsumer(): Promise<Consumer> {
  const kafkaConsumer = await createKafkaConsumer();

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      const payload = JSON.parse(message.value.toString()) as ShopifyOrderPayload;

      await wmsEtlQueue.add("transform-order", payload, {
        removeOnComplete: 100,
        removeOnFail: 100,
      });

      await inngest.send({
        name: "order/received",
        data: payload,
      });

      console.log(`[kafka] dispatched order ${payload.id}`);
    },
  });

  console.log("[kafka] consumer started");
  return kafkaConsumer;
}

/**
 * Starts the Kafka consumer for WMS inventory change events.
 *
 * @returns Connected Kafka consumer instance.
 * @throws When consumer startup fails.
 */
async function startInventoryChangeKafkaConsumer(): Promise<Consumer> {
  const kafkaConsumer = await createInventoryChangeConsumer();

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      const payload = JSON.parse(
        message.value.toString()
      ) as InventoryChangePayload;

      await inventoryChangeQueue.add("persist-inventory-change", payload, {
        removeOnComplete: 100,
        removeOnFail: 100,
      });

      console.log(
        `[kafka] dispatched inventory change sku=${payload.sku} delta=${payload.delta}`
      );
    },
  });

  console.log("[kafka] inventory change consumer started");
  return kafkaConsumer;
}

/**
 * Boots Kafka consumer, BullMQ worker, and Express server in order.
 *
 * @throws When any subsystem fails to start.
 */
async function bootstrap(): Promise<void> {
  await prisma.$connect();
  console.log("[prisma] database connected");

  consumer = await startKafkaConsumer();
  inventoryChangeConsumer = await startInventoryChangeKafkaConsumer();
  worker = startWmsEtlWorker();
  inventoryChangeWorker = startInventoryChangeWorker();
  console.log("[bullmq] workers started");

  producer = await createKafkaProducer();
  console.log("[kafka] producer connected");

  const app = createServer({ producer });
  httpServer = startServer(app);
}

/**
 * Gracefully shuts down all running subsystems.
 */
async function shutdown(): Promise<void> {
  console.log("[app] shutting down");

  await Promise.allSettled([
    consumer?.disconnect(),
    inventoryChangeConsumer?.disconnect(),
    producer?.disconnect(),
    worker?.close(),
    inventoryChangeWorker?.close(),
    wmsEtlQueue.close(),
    inventoryChangeQueue.close(),
    prisma.$disconnect(),
    new Promise<void>((resolve) => {
      if (!httpServer) {
        resolve();
        return;
      }

      httpServer.close(() => resolve());
    }),
  ]);
}

process.on("SIGINT", () => {
  shutdown()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error("[app] shutdown failed", error);
      process.exit(1);
    });
});

process.on("SIGTERM", () => {
  shutdown()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error("[app] shutdown failed", error);
      process.exit(1);
    });
});

bootstrap().catch((error) => {
  console.error("[app] bootstrap failed", error);
  process.exit(1);
});

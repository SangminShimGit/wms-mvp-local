import express, { type Express, type Request, type Response } from "express";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import { serve } from "inngest/express";
import type { Producer } from "kafkajs";
import { SHOPIFY_ORDERS_TOPIC } from "../config/kafka";
import { redis } from "../config/redis";
import { wmsEtlQueue } from "../workers/bullmq/worker";
import { inventoryChangeQueue } from "../workers/bullmq/inventoryChangeWorker";
import { inngest, orderWorkflow } from "../workers/inngest/client";
import { inventoryCriticalSync } from "../workers/inngest/inventoryCriticalSync";
import { inventorySync } from "../workers/inngest/inventorySync";

const IDEMPOTENCY_TTL_SECONDS = 86400;

export interface CreateServerOptions {
  producer: Producer;
}

/**
 * Builds the webhook idempotency key from Shopify headers or payload.
 *
 * @param req Incoming Express request.
 * @returns Redis key for webhook deduplication.
 */
function buildIdempotencyKey(req: Request): string {
  const webhookId =
    req.header("x-shopify-webhook-id") ??
    (req.body as { id?: number | string } | undefined)?.id ??
    "unknown";
  return `idemp:shopify:${webhookId}`;
}

/**
 * Creates and configures the Express application.
 *
 * @param options Server dependencies including Kafka producer.
 * @returns Configured Express application instance.
 */
export function createServer(options: CreateServerOptions): Express {
  const app = express();

  app.use(express.json());

  app.get("/health", (_req: Request, res: Response) => {
    res.status(200).json({ status: "ok" });
  });

  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath("/admin/queues");

  createBullBoard({
    queues: [
      new BullMQAdapter(wmsEtlQueue) as never,
      new BullMQAdapter(inventoryChangeQueue) as never,
    ],
    serverAdapter,
  });

  app.use("/admin/queues", serverAdapter.getRouter());

  app.use(
    "/api/inngest",
    serve({
      client: inngest,
      functions: [orderWorkflow, inventorySync, inventoryCriticalSync],
      serveHost: process.env.INNGEST_SERVE_HOST,
    })
  );

  app.post(
    "/webhooks/shopify/orders/create",
    async (req: Request, res: Response) => {
      try {
        const key = buildIdempotencyKey(req);
        const isNew = await redis.set(key, "1", "EX", IDEMPOTENCY_TTL_SECONDS, "NX");

        if (isNew === null) {
          res.status(200).json({ duplicated: true });
          return;
        }

        await options.producer.send({
          topic: SHOPIFY_ORDERS_TOPIC,
          messages: [{ value: JSON.stringify(req.body) }],
        });

        res.status(202).json({ accepted: true });
      } catch (error) {
        console.error("[api] webhook processing failed", error);
        res.status(500).json({ error: "Failed to process webhook" });
      }
    }
  );

  return app;
}

/**
 * Starts the HTTP server on the configured port.
 *
 * @param app Express application instance.
 * @returns Node HTTP server listening on PORT.
 */
export function startServer(app: Express) {
  const port = Number(process.env.PORT ?? 3000);

  return app.listen(port, () => {
    console.log(`[app] ready on :${port}`);
  });
}

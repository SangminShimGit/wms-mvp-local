import { Queue, Worker, type Job } from "bullmq";
import { Prisma } from "@prisma/client";
import { prisma } from "../../config/prisma";
import { getRedisUrl } from "../../config/redis";
import { inngest } from "../inngest/client";

export const INVENTORY_CHANGE_QUEUE_NAME = "inventory-change";

export interface InventoryChangePayload {
  sku: string;
  locationName: string;
  delta: number;
}

/**
 * Returns the stock level that triggers instant Shopify sync.
 *
 * @returns Critical stock threshold from environment (default 0).
 */
export function getCriticalStockThreshold(): number {
  const raw = process.env.CRITICAL_STOCK_THRESHOLD;
  if (raw === undefined || raw === "") {
    return 0;
  }

  const parsed = Number(raw);
  if (Number.isNaN(parsed)) {
    throw new Error("CRITICAL_STOCK_THRESHOLD must be a number");
  }

  return parsed;
}

const connection = { url: getRedisUrl() };

/**
 * BullMQ queue used for WMS inventory change ingestion jobs.
 */
export const inventoryChangeQueue = new Queue<InventoryChangePayload>(
  INVENTORY_CHANGE_QUEUE_NAME,
  { connection }
);

/**
 * Starts the BullMQ worker that persists inventory deltas and triggers critical sync.
 *
 * @returns Active BullMQ worker instance.
 */
export function startInventoryChangeWorker(): Worker<
  InventoryChangePayload,
  { sku: string; stock: number; critical: boolean }
> {
  const worker = new Worker<
    InventoryChangePayload,
    { sku: string; stock: number; critical: boolean }
  >(
    INVENTORY_CHANGE_QUEUE_NAME,
    async (job: Job<InventoryChangePayload>) => {
      const { sku, locationName, delta } = job.data;
      const threshold = getCriticalStockThreshold();

      const updated = await prisma.$transaction(async (tx: Prisma.TransactionClient) => {
        await tx.wmsInventoryChange.create({
          data: {
            sku,
            locationName,
            deltaQty: delta,
          },
        });

        return tx.wmsInventory.update({
          where: { sku },
          data: { stock: { increment: delta } },
        });
      });

      const critical = updated.stock <= threshold;

      if (critical) {
        await inngest.send({
          name: "inventory/critical.detected",
          data: { sku, locationName },
        });
      }

      console.log(
        `[bullmq] inventory change sku=${sku} delta=${delta} stock=${updated.stock} critical=${critical}`
      );

      return {
        sku: updated.sku,
        stock: updated.stock,
        critical,
      };
    },
    { connection }
  );

  worker.on("completed", (job) => {
    console.log(`[bullmq] inventory job ${job.id} completed`);
  });

  worker.on("failed", (job, error) => {
    console.error(`[bullmq] inventory job ${job?.id} failed`, error);
  });

  return worker;
}

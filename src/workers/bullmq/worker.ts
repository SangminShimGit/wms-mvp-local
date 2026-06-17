import { Queue, Worker, type Job } from "bullmq";
import { prisma } from "../../config/prisma";
import { getRedisUrl } from "../../config/redis";

export const WMS_ETL_QUEUE_NAME = "wms-etl";

export interface ShopifyOrderPayload {
  id: number | string;
  line_items?: Array<Record<string, unknown>>;
  [key: string]: unknown;
}

export interface WmsOrderPayload {
  externalOrderId: string;
  source: "shopify";
  lineItemCount: number;
  transformedAt: string;
}

/**
 * Transforms a Shopify order payload into a WMS-compatible order payload.
 *
 * @param order Raw Shopify order payload.
 * @returns Normalized WMS order payload.
 */
export function transformShopifyOrderToWms(
  order: ShopifyOrderPayload
): WmsOrderPayload {
  return {
    externalOrderId: String(order.id),
    source: "shopify",
    lineItemCount: order.line_items?.length ?? 0,
    transformedAt: new Date().toISOString(),
  };
}

const connection = { url: getRedisUrl() };

/**
 * BullMQ queue used for Shopify-to-WMS ETL jobs.
 */
export const wmsEtlQueue = new Queue(WMS_ETL_QUEUE_NAME, { connection });

/**
 * Starts the BullMQ worker that executes Shopify-to-WMS ETL jobs.
 *
 * @returns Active BullMQ worker instance.
 */
export function startWmsEtlWorker(): Worker<ShopifyOrderPayload, WmsOrderPayload> {
  const worker = new Worker<ShopifyOrderPayload, WmsOrderPayload>(
    WMS_ETL_QUEUE_NAME,
    async (job: Job<ShopifyOrderPayload>) => {
      console.log(`[bullmq] processing job ${job.id} for order ${job.data.id}`);
      const wmsOrder = transformShopifyOrderToWms(job.data);
      const persisted = await prisma.wmsOrder.create({
        data: {
          shopifyId: wmsOrder.externalOrderId,
          status: "PENDING",
        },
      });
      console.log(
        `[bullmq] persisted order ${persisted.shopifyId} (id=${persisted.id})`
      );
      return wmsOrder;
    },
    { connection }
  );

  worker.on("completed", (job) => {
    console.log(`[bullmq] job ${job.id} completed`);
  });

  worker.on("failed", (job, error) => {
    console.error(`[bullmq] job ${job?.id} failed`, error);
  });

  return worker;
}

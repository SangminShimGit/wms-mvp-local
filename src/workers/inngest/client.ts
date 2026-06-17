import { Inngest } from "inngest";
import { prisma } from "../../config/prisma";

export interface OrderReceivedEventData {
  id: number | string;
  line_items?: Array<Record<string, unknown>>;
  [key: string]: unknown;
}

export interface InventoryCriticalDetectedEventData {
  sku: string;
  locationName?: string;
}

/**
 * Inngest client configured for local dev server integration.
 */
export const inngest = new Inngest({
  id: "wms-mvp-local",
  eventKey: process.env.INNGEST_EVENT_KEY,
});

/**
 * Order validation workflow that waits 10 seconds before completing validation
 * and finalizing WMS allocation in the database.
 */
export const orderWorkflow = inngest.createFunction(
  { id: "order-workflow" },
  { event: "order/received" },
  async ({ event, step }) => {
    await step.sleep("wait-validation", "10s");

    const validated = await step.run("validate", async () => {
      const orderId = event.data.id;
      if (orderId === undefined || orderId === null) {
        throw new Error("Order id is required for validation");
      }

      return {
        ok: true,
        orderId: String(orderId),
        validatedAt: new Date().toISOString(),
      };
    });

    const finalized = await step.run("finalize-wms-allocation", async () => {
      const updated = await prisma.wmsOrder.update({
        where: { shopifyId: String(event.data.id) },
        data: { status: "READY_TO_SHIP" },
      });

      return {
        shopifyId: updated.shopifyId,
        status: updated.status,
      };
    });

    return {
      status: "completed",
      validated,
      finalized,
    };
  }
);

import { pushPendingDeltas } from "../../services/shopifyInventoryAdjust";
import { inngest } from "./client";

/**
 * Instant pending delta push for critical stock levels (stock at or below threshold).
 */
export const inventoryCriticalSync = inngest.createFunction(
  { id: "inventory-critical-sync", name: "Instant pending delta push (critical)" },
  { event: "inventory/critical.detected" },
  async ({ event, step }) => {
    const result = await step.run("push-critical", async () =>
      pushPendingDeltas({ sku: event.data.sku })
    );

    return result;
  }
);

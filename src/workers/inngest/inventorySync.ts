import { pushPendingDeltas } from "../../services/shopifyInventoryAdjust";
import { inngest } from "./client";

/**
 * Bulk inventory sync workflow triggered every 15 minutes or manually via event.
 */
export const inventorySync = inngest.createFunction(
  { id: "inventory-sync", name: "Bulk inventory sync to Shopify" },
  [{ cron: "*/15 * * * *" }, { event: "inventory/sync.requested" }],
  async ({ step }) => {
    const result = await step.run("push-pending-deltas", async () =>
      pushPendingDeltas()
    );

    return result;
  }
);

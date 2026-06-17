import "dotenv/config";
import {
  createKafkaProducer,
  WMS_INVENTORY_CHANGES_TOPIC,
} from "../src/config/kafka";
import type { InventoryChangePayload } from "../src/workers/bullmq/inventoryChangeWorker";

const DEFAULT_SKU = "sku-managed-1";
const DEFAULT_LOCATION = "Shop location";
const DEFAULT_DELTA = -5;

/**
 * Parses an integer CLI argument or environment variable.
 *
 * @param envValue Raw environment variable value.
 * @param argValue Raw CLI argument value.
 * @param fallback Default when neither is set.
 * @returns Parsed integer.
 * @throws When the provided value is not a valid integer.
 */
function parseIntArg(
  envValue: string | undefined,
  argValue: string | undefined,
  fallback: number
): number {
  const raw = argValue ?? envValue;
  if (raw === undefined || raw === "") {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isInteger(parsed)) {
    throw new Error(`delta must be an integer, got: ${raw}`);
  }

  return parsed;
}

/**
 * Publishes a WMS inventory change message to Kafka for local pipeline testing.
 *
 * Defaults: sku `sku-managed-1`, location `Shop location`, delta `-5`.
 * Override via env (`TEST_SKU`, `TEST_LOCATION`, `TEST_DELTA`) or CLI:
 * `npx tsx scripts/publish-inventory-change.ts [delta]`
 *
 * Host dev: set `KAFKA_BROKER=localhost:9094` in `.env` (Docker maps PLAINTEXT_HOST).
 *
 * @returns Resolves when the message is sent and the producer disconnects.
 * @throws When Kafka connection or publish fails.
 */
async function main(): Promise<void> {
  const delta = parseIntArg(
    process.env.TEST_DELTA,
    process.argv[2],
    DEFAULT_DELTA
  );

  const payload: InventoryChangePayload = {
    sku: process.env.TEST_SKU ?? DEFAULT_SKU,
    locationName: process.env.TEST_LOCATION ?? DEFAULT_LOCATION,
    delta,
  };

  const producer = await createKafkaProducer();

  try {
    await producer.send({
      topic: WMS_INVENTORY_CHANGES_TOPIC,
      messages: [{ value: JSON.stringify(payload) }],
    });

    console.log("Published inventory change:");
    console.log(JSON.stringify(payload, null, 2));
    console.log(`Topic: ${WMS_INVENTORY_CHANGES_TOPIC}`);
    console.log(`Broker: ${process.env.KAFKA_BROKER ?? "localhost:9092"}`);
    console.log("");
    console.log("Next checks:");
    console.log("  docker compose logs app  -> [kafka] dispatched inventory change ...");
    console.log("  http://localhost:3000/admin/queues -> inventory-change completed");
    console.log("  Studio -> wms_inventory_changes, WmsInventory");
  } finally {
    await producer.disconnect();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

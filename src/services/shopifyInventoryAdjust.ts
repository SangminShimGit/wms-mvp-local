import { createHash } from "crypto";
import { prisma } from "../config/prisma";
import { shopifyGraphQL } from "./shopify";

export interface PushPendingDeltasOptions {
  sku?: string;
}

export interface PushPendingDeltasResult {
  pushed: number;
  attempted: number;
  skipped: boolean;
  reason?: string;
  failed: number;
  failedReasons: Array<{
    sourceIds: string[];
    message: string;
    code?: string;
  }>;
}

interface AggregatedDelta {
  sku: string;
  locationName: string;
  deltaQty: number;
  sourceIds: bigint[];
}

const SKU_CHUNK_SIZE = 50;
const ITEM_CHUNK_SIZE = 100;
const MUTATION_CHUNK_SIZE = 10;

/**
 * Splits an array into fixed-size chunks for batched GraphQL requests.
 *
 * @param items Source array to split.
 * @param size Maximum chunk length.
 * @returns Array of chunk arrays.
 */
function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }
  return out;
}

/**
 * Escapes single quotes in a SKU for Shopify search query syntax.
 *
 * @param sku Raw SKU value.
 * @returns Escaped SKU safe for `sku:'...'` search terms.
 */
function escapeSkuForSearch(sku: string): string {
  return sku.replace(/'/g, "\\'");
}

/**
 * Fetches unpushed inventory change rows and aggregates them by SKU and location.
 *
 * @param sku Optional SKU filter for critical single-shot sync.
 * @returns Aggregated delta groups with source row IDs.
 */
async function fetchAggregatedDeltas(
  sku?: string
): Promise<AggregatedDelta[]> {
  const rows = await prisma.wmsInventoryChange.findMany({
    where: {
      isShopifyPushed: false,
      ...(sku ? { sku } : {}),
    },
    orderBy: { createdAt: "asc" },
    take: 500,
  });

  const grouped = new Map<string, AggregatedDelta>();

  for (const row of rows) {
    const key = `${row.sku}::${row.locationName}`;
    const existing = grouped.get(key);

    if (existing) {
      existing.deltaQty += row.deltaQty;
      existing.sourceIds.push(row.id);
      continue;
    }

    grouped.set(key, {
      sku: row.sku,
      locationName: row.locationName,
      deltaQty: row.deltaQty,
      sourceIds: [row.id],
    });
  }

  return [...grouped.values()].filter((group) => group.deltaQty !== 0);
}

/**
 * Resolves Shopify location names to GraphQL location IDs.
 *
 * @param locationNames Distinct WMS location names to resolve.
 * @returns Map of location name to Shopify location ID.
 */
async function resolveLocationMap(
  locationNames: string[]
): Promise<Record<string, string>> {
  const data = await shopifyGraphQL<{
    locations: { nodes: { id: string; name: string }[] };
  }>(`query { locations(first: 50) { nodes { id name } } }`, {});

  const allLocations = Object.fromEntries(
    data.locations.nodes.map((location) => [location.name, location.id])
  );

  return Object.fromEntries(
    locationNames
      .map((name) => [name, allLocations[name]])
      .filter((entry): entry is [string, string] => Boolean(entry[1]))
  );
}

/**
 * Resolves Shopify inventory item IDs for the given SKUs.
 *
 * Uses a single `productVariants` query per chunk with OR-combined SKU search terms.
 *
 * @param skus Distinct SKUs to resolve.
 * @returns Map of SKU to Shopify inventory item ID.
 */
async function resolveVariantMap(
  skus: string[]
): Promise<Record<string, string>> {
  const uniqueSkus = [...new Set(skus)];
  if (uniqueSkus.length === 0) {
    return {};
  }

  const result: Record<string, string> = {};

  const chunkResults = await Promise.all(
    chunk(uniqueSkus, SKU_CHUNK_SIZE).map(async (chunkSkus) => {
      const q = chunkSkus
        .map((sku) => `sku:'${escapeSkuForSearch(sku)}'`)
        .join(" OR ");

      const data = await shopifyGraphQL<{
        productVariants: {
          nodes: { sku: string; inventoryItem: { id: string } | null }[];
        };
      }>(
        `query($q: String!, $first: Int!) {
          productVariants(first: $first, query: $q) {
            nodes { sku inventoryItem { id } }
          }
        }`,
        { q, first: chunkSkus.length }
      );

      return data.productVariants.nodes;
    })
  );

  for (const nodes of chunkResults) {
    for (const node of nodes) {
      if (node.sku && node.inventoryItem?.id) {
        result[node.sku] = node.inventoryItem.id;
      }
    }
  }

  return result;
}

/**
 * Resolves current Shopify on_hand quantity per inventory item and location.
 *
 * Uses `nodes(ids:)` to fetch inventory levels in bulk; assumes each item has at most
 * 50 locations (MVP single-location shops). Add pagination if multi-location scale grows.
 *
 * @param pairs Distinct inventory item and location ID pairs.
 * @returns Map keyed by `inventoryItemId::locationId` to on_hand quantity.
 */
async function resolveOnHandMap(
  pairs: Array<{ inventoryItemId: string; locationId: string }>
): Promise<Record<string, number>> {
  const wantedKeys = new Set<string>();
  const uniqueItemIds = new Set<string>();

  for (const pair of pairs) {
    wantedKeys.add(`${pair.inventoryItemId}::${pair.locationId}`);
    uniqueItemIds.add(pair.inventoryItemId);
  }

  if (uniqueItemIds.size === 0) {
    return {};
  }

  const result: Record<string, number> = {};

  const chunkResults = await Promise.all(
    chunk([...uniqueItemIds], ITEM_CHUNK_SIZE).map(async (chunkIds) => {
      const data = await shopifyGraphQL<{
        nodes: Array<{
          id: string;
          inventoryLevels: {
            nodes: Array<{
              location: { id: string };
              quantities: { name: string; quantity: number }[];
            }>;
          };
        } | null>;
      }>(
        `query($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on InventoryItem {
              id
              inventoryLevels(first: 50) {
                nodes {
                  location { id }
                  quantities(names: ["on_hand"]) { name quantity }
                }
              }
            }
          }
        }`,
        { ids: chunkIds }
      );

      return data.nodes;
    })
  );

  for (const nodes of chunkResults) {
    for (const node of nodes) {
      if (!node) {
        continue;
      }

      for (const level of node.inventoryLevels.nodes) {
        const key = `${node.id}::${level.location.id}`;
        if (!wantedKeys.has(key)) {
          continue;
        }

        const onHand =
          level.quantities.find((q) => q.name === "on_hand")?.quantity ?? 0;
        result[key] = onHand;
      }
    }
  }

  return result;
}

/**
 * Pushes pending WMS inventory deltas to Shopify via inventorySetQuantities (2026-04 on_hand).
 *
 * Processes mutations in chunks of {@link MUTATION_CHUNK_SIZE} sequentially. Failed chunks
 * are isolated — successful chunks are partially marked `isShopifyPushed = true`.
 *
 * @param options Optional SKU filter for critical single-shot sync.
 * @returns Count of pushed/failed adjustments and per-chunk failure details.
 * @throws When bulk resolve steps fail or required credentials are missing.
 */
export async function pushPendingDeltas(
  options: PushPendingDeltasOptions = {}
): Promise<PushPendingDeltasResult> {
  const aggregated = await fetchAggregatedDeltas(options.sku);

  if (aggregated.length === 0) {
    return {
      pushed: 0,
      attempted: 0,
      skipped: true,
      reason: "no-pending-deltas",
      failed: 0,
      failedReasons: [],
    };
  }

  const attempted = aggregated.reduce(
    (count, group) => count + group.sourceIds.length,
    0
  );

  const locationMap = await resolveLocationMap([
    ...new Set(aggregated.map((group) => group.locationName)),
  ]);
  const variantMap = await resolveVariantMap([
    ...new Set(aggregated.map((group) => group.sku)),
  ]);

  const changes = aggregated
    .map((group) => ({
      delta: group.deltaQty,
      inventoryItemId: variantMap[group.sku],
      locationId: locationMap[group.locationName],
      sourceIds: group.sourceIds,
    }))
    .filter((entry) => entry.inventoryItemId && entry.locationId);

  if (changes.length === 0) {
    return {
      pushed: 0,
      attempted,
      skipped: true,
      reason: "no-resolvable-deltas",
      failed: 0,
      failedReasons: [],
    };
  }

  const onHandMap = await resolveOnHandMap(
    changes.map(({ inventoryItemId, locationId }) => ({
      inventoryItemId: inventoryItemId as string,
      locationId: locationId as string,
    }))
  );

  const mutationChanges = changes.map(
    ({ delta, inventoryItemId, locationId, sourceIds }) => {
      const key = `${inventoryItemId}::${locationId}`;
      const changeFromQuantity = onHandMap[key] ?? 0;
      return {
        inventoryItemId,
        locationId,
        quantity: changeFromQuantity + delta,
        changeFromQuantity,
        delta,
        sourceIds,
      };
    }
  );

  const chunks = chunk(mutationChanges, MUTATION_CHUNK_SIZE);
  let pushed = 0;
  let failed = 0;
  const failedReasons: PushPendingDeltasResult["failedReasons"] = [];

  for (const chunkChanges of chunks) {
    const chunkSourceIds = chunkChanges.flatMap((change) => change.sourceIds);
    const idempotencyKey = createHash("sha256")
      .update(chunkSourceIds.map(String).sort().join(","))
      .digest("hex");

    try {
      const data = await shopifyGraphQL<{
        inventorySetQuantities: {
          userErrors: { field: string[]; message: string; code?: string }[];
        };
      }>(
        `mutation($input: InventorySetQuantitiesInput!, $idempotencyKey: String!) {
          inventorySetQuantities(input: $input) @idempotent(key: $idempotencyKey) {
            inventoryAdjustmentGroup { createdAt reason }
            userErrors { field message code }
          }
        }`,
        {
          input: {
            name: "on_hand",
            reason: "correction",
            quantities: chunkChanges.map(
              ({
                quantity,
                inventoryItemId,
                locationId,
                changeFromQuantity,
              }) => ({
                quantity,
                inventoryItemId,
                locationId,
                changeFromQuantity,
              })
            ),
          },
          idempotencyKey,
        }
      );

      if (data.inventorySetQuantities.userErrors.length > 0) {
        failed += chunkSourceIds.length;
        failedReasons.push({
          sourceIds: chunkSourceIds.map(String),
          message: JSON.stringify(data.inventorySetQuantities.userErrors),
          code: data.inventorySetQuantities.userErrors[0]?.code,
        });
        continue;
      }

      await prisma.wmsInventoryChange.updateMany({
        where: { id: { in: chunkSourceIds } },
        data: { isShopifyPushed: true },
      });
      pushed += chunkChanges.length;
    } catch (error) {
      failed += chunkSourceIds.length;
      failedReasons.push({
        sourceIds: chunkSourceIds.map(String),
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return {
    pushed,
    attempted,
    skipped: false,
    failed,
    failedReasons,
  };
}

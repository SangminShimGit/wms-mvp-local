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
}

interface AggregatedDelta {
  sku: string;
  locationName: string;
  deltaQty: number;
  sourceIds: bigint[];
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
 * @param skus Distinct SKUs to resolve.
 * @returns Map of SKU to Shopify inventory item ID.
 */
async function resolveVariantMap(
  skus: string[]
): Promise<Record<string, string>> {
  const result: Record<string, string> = {};

  for (const sku of skus) {
    const data = await shopifyGraphQL<{
      productVariants: {
        nodes: { sku: string; inventoryItem: { id: string } }[];
      };
    }>(
      `query($q: String!) {
        productVariants(first: 1, query: $q) {
          nodes { sku inventoryItem { id } }
        }
      }`,
      { q: `sku:'${sku}'` }
    );

    const node = data.productVariants.nodes[0];
    if (node?.inventoryItem?.id) {
      result[sku] = node.inventoryItem.id;
    }
  }

  return result;
}

/**
 * Resolves current Shopify on_hand quantity per inventory item and location.
 *
 * @param pairs Distinct inventory item and location ID pairs.
 * @returns Map keyed by `inventoryItemId::locationId` to on_hand quantity.
 */
async function resolveOnHandMap(
  pairs: Array<{ inventoryItemId: string; locationId: string }>
): Promise<Record<string, number>> {
  const result: Record<string, number> = {};
  const uniqueKeys = new Set<string>();

  for (const pair of pairs) {
    const key = `${pair.inventoryItemId}::${pair.locationId}`;
    if (uniqueKeys.has(key)) {
      continue;
    }
    uniqueKeys.add(key);

    const data = await shopifyGraphQL<{
      inventoryItem: {
        inventoryLevel: {
          quantities: { name: string; quantity: number }[];
        } | null;
      } | null;
    }>(
      `query($inventoryItemId: ID!, $locationId: ID!) {
        inventoryItem(id: $inventoryItemId) {
          inventoryLevel(locationId: $locationId) {
            quantities(names: ["on_hand"]) {
              name
              quantity
            }
          }
        }
      }`,
      {
        inventoryItemId: pair.inventoryItemId,
        locationId: pair.locationId,
      }
    );

    const onHand =
      data.inventoryItem?.inventoryLevel?.quantities.find(
        (q) => q.name === "on_hand"
      )?.quantity ?? 0;
    result[key] = onHand;
  }

  return result;
}

/**
 * Pushes pending WMS inventory deltas to Shopify via inventorySetQuantities (2026-04 on_hand).
 *
 * @param options Optional SKU filter for critical single-shot sync.
 * @returns Count of pushed adjustments and source rows attempted.
 * @throws When Shopify returns userErrors or required credentials are missing.
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
    };
  }

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
      attempted: aggregated.reduce(
        (count, group) => count + group.sourceIds.length,
        0
      ),
      skipped: true,
      reason: "no-resolvable-deltas",
    };
  }

  const onHandMap = await resolveOnHandMap(
    changes.map(({ inventoryItemId, locationId }) => ({
      inventoryItemId: inventoryItemId as string,
      locationId: locationId as string,
    }))
  );

  const mutationChanges = changes.map(
    ({ delta, inventoryItemId, locationId }) => {
      const key = `${inventoryItemId}::${locationId}`;
      const changeFromQuantity = onHandMap[key] ?? 0;
      return {
        inventoryItemId,
        locationId,
        quantity: changeFromQuantity + delta,
        changeFromQuantity,
        delta,
      };
    }
  );

  const sourceIds = changes.flatMap((change) => change.sourceIds);
  const idempotencyKey = createHash("sha256")
    .update(sourceIds.map(String).sort().join(","))
    .digest("hex");

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
        quantities: mutationChanges.map(
          ({ quantity, inventoryItemId, locationId, changeFromQuantity }) => ({
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
    throw new Error(
      `Shopify userErrors: ${JSON.stringify(data.inventorySetQuantities.userErrors)}`
    );
  }

  await prisma.wmsInventoryChange.updateMany({
    where: { id: { in: sourceIds } },
    data: { isShopifyPushed: true },
  });

  return {
    pushed: changes.length,
    attempted: aggregated.reduce(
      (count, group) => count + group.sourceIds.length,
      0
    ),
    skipped: false,
  };
}

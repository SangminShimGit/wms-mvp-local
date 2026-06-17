/**
 * Builds the Shopify Admin GraphQL endpoint URL from environment variables.
 *
 * @returns Shopify GraphQL API URL.
 * @throws When SHOPIFY_SHOP_DOMAIN is not configured.
 */
export function getShopifyEndpoint(): string {
  const domain = process.env.SHOPIFY_SHOP_DOMAIN;
  if (!domain) {
    throw new Error("SHOPIFY_SHOP_DOMAIN is required");
  }

  const version = process.env.SHOPIFY_API_VERSION ?? "2026-04";
  return `https://${domain}/admin/api/${version}/graphql.json`;
}

/**
 * Executes a Shopify Admin GraphQL request.
 *
 * @param query GraphQL query or mutation string.
 * @param variables GraphQL variables payload.
 * @returns Parsed GraphQL data payload.
 * @throws When credentials are missing, HTTP fails, or GraphQL returns errors.
 */
export async function shopifyGraphQL<T>(
  query: string,
  variables: Record<string, unknown>
): Promise<T> {
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!token) {
    throw new Error("SHOPIFY_ADMIN_ACCESS_TOKEN is required");
  }

  const response = await fetch(getShopifyEndpoint(), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": token,
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!response.ok) {
    throw new Error(`Shopify ${response.status}: ${await response.text()}`);
  }

  const body = (await response.json()) as { data?: T; errors?: unknown };
  if (body.errors) {
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(body.errors)}`);
  }

  return body.data as T;
}

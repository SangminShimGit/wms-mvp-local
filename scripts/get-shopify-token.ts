import "dotenv/config";
import { execFile } from "node:child_process";
import crypto from "node:crypto";
import http from "node:http";
import { URL } from "node:url";

const SHOP = process.env.SHOPIFY_SHOP_DOMAIN;
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
const SCOPES =
  process.env.SHOPIFY_SCOPES ?? "read_locations,read_products,write_inventory";
const PORT = Number(process.env.SHOPIFY_OAUTH_PORT ?? 53682);
const REDIRECT_URI = `http://localhost:${PORT}/callback`;
const TIMEOUT_MS = 5 * 60 * 1000;

/**
 * Validates required Shopify OAuth environment variables.
 *
 * @throws When any required variable is missing.
 */
function validateEnv(): void {
  const missing: string[] = [];

  if (!SHOP) {
    missing.push("SHOPIFY_SHOP_DOMAIN");
  }
  if (!CLIENT_ID) {
    missing.push("SHOPIFY_CLIENT_ID");
  }
  if (!CLIENT_SECRET) {
    missing.push("SHOPIFY_CLIENT_SECRET");
  }

  if (missing.length > 0) {
    console.error(
      `Missing required environment variable(s): ${missing.join(", ")}`
    );
    console.error(
      "Set them in .env (copy from .env.example) and re-run: npm run shopify:token"
    );
    process.exit(1);
  }

  if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
    console.error(
      `Invalid SHOPIFY_OAUTH_PORT: ${process.env.SHOPIFY_OAUTH_PORT ?? "(default 53682)"}`
    );
    process.exit(1);
  }
}

/**
 * Builds the Shopify OAuth install URL for the authorization code grant.
 *
 * @param state CSRF nonce echoed back on callback.
 * @returns Full authorize URL to open in a browser.
 */
function buildInstallUrl(state: string): string {
  const params = new URLSearchParams({
    client_id: CLIENT_ID!,
    scope: SCOPES,
    redirect_uri: REDIRECT_URI,
    state,
  });

  return `https://${SHOP}/admin/oauth/authorize?${params.toString()}`;
}

/**
 * Verifies the Shopify OAuth callback HMAC signature.
 *
 * @param queryParams Parsed callback query parameters.
 * @param secret Shopify app client secret used as HMAC key.
 * @returns True when the supplied hmac matches the computed digest.
 */
function verifyHmac(
  queryParams: URLSearchParams,
  secret: string
): boolean {
  const suppliedHmac = queryParams.get("hmac");
  if (!suppliedHmac) {
    return false;
  }

  const message = [...queryParams.entries()]
    .filter(([key]) => key !== "hmac" && key !== "signature")
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");

  const digest = crypto
    .createHmac("sha256", secret)
    .update(message)
    .digest("hex");

  try {
    return crypto.timingSafeEqual(
      Buffer.from(digest, "utf8"),
      Buffer.from(suppliedHmac, "utf8")
    );
  } catch {
    return false;
  }
}

/**
 * Exchanges an OAuth authorization code for a Shopify Admin API access token.
 *
 * @param shop Shop domain (e.g. store.myshopify.com).
 * @param code Authorization code from the OAuth callback.
 * @returns Access token payload from Shopify.
 * @throws When the token exchange HTTP request fails or returns an error body.
 */
async function exchangeCode(
  shop: string,
  code: string
): Promise<{ access_token: string; scope: string }> {
  const response = await fetch(`https://${shop}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code,
      redirect_uri: REDIRECT_URI,
    }),
  });

  const bodyText = await response.text();
  if (!response.ok) {
    throw new Error(`Token exchange failed (${response.status}): ${bodyText}`);
  }

  const body = JSON.parse(bodyText) as {
    access_token?: string;
    scope?: string;
    error?: string;
    error_description?: string;
  };

  if (body.error) {
    throw new Error(
      `Token exchange error: ${body.error}${body.error_description ? ` - ${body.error_description}` : ""}`
    );
  }

  if (!body.access_token) {
    throw new Error(`Token exchange returned no access_token: ${bodyText}`);
  }

  return {
    access_token: body.access_token,
    scope: body.scope ?? SCOPES,
  };
}

/**
 * Best-effort browser open; always prints the URL as fallback.
 *
 * Uses rundll32 on Windows so query strings with `&` are not truncated by cmd.exe.
 *
 * @param url OAuth install URL to open.
 */
function openBrowser(url: string): void {
  console.log(`\nOpen this URL in your browser if it did not open automatically:\n${url}\n`);
  console.log(
    `Ensure ${REDIRECT_URI} is listed under Allowed redirect URLs in your Shopify Partner app.\n`
  );

  const open = (command: string, args: string[]): void => {
    execFile(command, args, { detached: true, stdio: "ignore" }, () => undefined).unref();
  };

  if (process.platform === "win32") {
    open("rundll32", ["url.dll,FileProtocolHandler", url]);
  } else if (process.platform === "darwin") {
    open("open", [url]);
  } else {
    open("xdg-open", [url]);
  }
}

/**
 * Sends a minimal HTML response to the OAuth callback browser tab.
 *
 * @param res HTTP response object.
 * @param status HTTP status code.
 * @param title Page title and heading text.
 * @param message Body message shown to the user.
 */
function sendHtml(
  res: http.ServerResponse,
  status: number,
  title: string,
  message: string
): void {
  res.writeHead(status, { "Content-Type": "text/html; charset=utf-8" });
  res.end(
    `<!doctype html><html><head><title>${title}</title></head><body><h1>${title}</h1><p>${message}</p></body></html>`
  );
}

/**
 * Runs the Shopify OAuth authorization code flow and prints the Admin token.
 */
async function main(): Promise<void> {
  validateEnv();

  const state = crypto.randomBytes(16).toString("hex");
  const installUrl = buildInstallUrl(state);

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      server.close();
      reject(new Error("Timed out after 5 minutes waiting for OAuth callback."));
    }, TIMEOUT_MS);

    const server = http.createServer(async (req, res) => {
      if (req.method !== "GET" || req.url?.split("?")[0] !== "/callback") {
        sendHtml(res, 404, "Not Found", "Expected GET /callback.");
        return;
      }

      const callbackUrl = new URL(req.url, `http://localhost:${PORT}`);
      const queryParams = callbackUrl.searchParams;

      const fail = (message: string): void => {
        clearTimeout(timeout);
        sendHtml(res, 400, "Authorization Failed", message);
        server.close();
        reject(new Error(message));
      };

      const callbackState = queryParams.get("state");
      if (!callbackState) {
        fail("Missing state parameter.");
        return;
      }

      try {
        if (
          !crypto.timingSafeEqual(
            Buffer.from(callbackState, "utf8"),
            Buffer.from(state, "utf8")
          )
        ) {
          fail("State mismatch — possible CSRF. Try again.");
          return;
        }
      } catch {
        fail("State mismatch — possible CSRF. Try again.");
        return;
      }

      if (!verifyHmac(queryParams, CLIENT_SECRET!)) {
        fail("Invalid HMAC signature.");
        return;
      }

      const code = queryParams.get("code");
      const shop = queryParams.get("shop") ?? SHOP!;
      if (!code) {
        fail("Missing authorization code.");
        return;
      }

      try {
        const token = await exchangeCode(shop, code);
        clearTimeout(timeout);

        sendHtml(
          res,
          200,
          "Authorization Successful",
          "You can close this tab and return to the terminal."
        );

        console.log(`SHOPIFY_ADMIN_ACCESS_TOKEN=${token.access_token}`);
        console.log(`# granted scopes: ${token.scope}`);

        server.close();
        resolve();
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Token exchange failed.";
        fail(message);
      }
    });

    server.listen(PORT, "127.0.0.1", () => {
      openBrowser(installUrl);
    });

    server.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });

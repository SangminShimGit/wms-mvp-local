var _a;
import http from "k6/http";
import { check } from "k6";
export const options = {
    vus: 100,
    duration: "15s",
};
const TARGET_URL = (_a = __ENV.TARGET_URL) !== null && _a !== void 0 ? _a : "http://localhost:3000/webhooks/shopify/orders/create";
const FIXED_DUPLICATE_ID = 999999999;
const HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Topic": "orders/create",
    "X-Shopify-Hmac-Sha256": "local-stress-test-hmac-signature",
    "X-Shopify-Shop-Domain": "test-wms-store.myshopify.com",
};
/**
 * Builds a fake Shopify order payload.
 *
 * @param orderId Shopify-style order id to embed in the payload.
 * @returns JSON-stringified order body matching the webhook contract.
 */
function buildPayload(orderId) {
    return JSON.stringify({
        id: orderId,
        email: `stress+${orderId}@example.com`,
        total_price: (Math.random() * 1000).toFixed(2),
        line_items: [
            {
                id: orderId * 10 + 1,
                title: "Stress SKU A",
                quantity: 1,
                price: "19.99",
            },
            {
                id: orderId * 10 + 2,
                title: "Stress SKU B",
                quantity: 2,
                price: "9.99",
            },
        ],
    });
}
/**
 * Single k6 iteration: 50% unique-id traffic, 50% duplicate-id traffic.
 */
export default function () {
    const useDuplicate = Math.random() < 0.5;
    const orderId = useDuplicate
        ? FIXED_DUPLICATE_ID
        : Math.floor(Math.random() * 10000000);
    const res = http.post(TARGET_URL, buildPayload(orderId), { headers: HEADERS });
    check(res, {
        "status is 200 or 202": (r) => r.status === 200 || r.status === 202,
        "unique -> 202 accepted": (r) => useDuplicate || r.status === 202,
        "duplicate -> 200 duplicated": (r) => !useDuplicate ||
            (r.status === 200 && r.body.indexOf("duplicated") >= 0),
    });
}

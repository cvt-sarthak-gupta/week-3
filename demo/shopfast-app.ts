/**
 * ShopFast — a dummy e-commerce API instrumented with PulseBoard.
 *
 * Endpoints:
 *   GET  /products          → list products
 *   GET  /products/:id      → product detail (occasionally 404)
 *   POST /cart/add          → add item to cart
 *   POST /checkout          → place order (payment failures 20% of the time)
 *   GET  /health            → health check
 *
 * Every request is traced to PulseBoard automatically.
 */

import Fastify from 'fastify';
import { PulseBoard } from './pulseboard.js';

const PRODUCTS = [
  { id: 'p1', name: 'Mechanical Keyboard', price: 149.99, stock: 12 },
  { id: 'p2', name: 'USB-C Hub',           price: 49.99,  stock: 0  },  // out of stock
  { id: 'p3', name: 'Monitor Stand',       price: 89.99,  stock: 7  },
  { id: 'p4', name: 'Desk Mat XL',         price: 34.99,  stock: 30 },
  { id: 'p5', name: 'Webcam 4K',           price: 199.99, stock: 3  },
];

let orderCount = 0;

export async function createShopFastApp(pb: PulseBoard, port: number): Promise<() => Promise<void>> {
  const app = Fastify({ logger: false });

  // No blanket request-level metrics hook — that would flood the rate limit
  // window with info events and crowd out meaningful error/business events.
  // Each route captures its own events explicitly instead.

  // ── Routes ──────────────────────────────────────────────────────────────────
  app.get('/health', async (_req, reply) => {
    void reply.send({ status: 'ok', service: 'shopfast' });
  });

  app.get('/products', async (_req, reply) => {
    pb.capture({
      type: 'log',
      severity: 'info',
      message: 'Product catalogue fetched',
      tags: { env: 'production', service: 'shopfast' },
      payload: { count: PRODUCTS.length },
    });
    void reply.send({ products: PRODUCTS });
  });

  app.get<{ Params: { id: string } }>('/products/:id', async (req, reply) => {
    const product = PRODUCTS.find((p) => p.id === req.params.id);
    if (!product) {
      pb.captureError(new Error(`Product not found: ${req.params.id}`), {
        severity: 'warn',
        tags: { env: 'production', service: 'shopfast' },
        userContext: { ip: req.ip },
      });
      void reply.status(404).send({ error: 'Product not found' });
      return;
    }

    if (product.stock === 0) {
      pb.capture({
        type: 'log',
        severity: 'warn',
        message: `Out-of-stock product viewed: ${product.name}`,
        tags: { env: 'production', service: 'shopfast', productId: product.id },
      });
    }

    void reply.send(product);
  });

  app.post<{ Body: { productId: string; qty: number; userId: string } }>(
    '/cart/add',
    async (req, reply) => {
      const { productId, qty, userId } = req.body;
      const product = PRODUCTS.find((p) => p.id === productId);

      if (!product) {
        const err = new Error(`AddToCart: unknown product ${productId}`);
        pb.captureError(err, {
          severity: 'error',
          tags: { env: 'production', service: 'shopfast' },
          userContext: { userId },
        });
        void reply.status(400).send({ error: 'Unknown product' });
        return;
      }

      if (product.stock < qty) {
        pb.capture({
          type: 'log',
          severity: 'warn',
          message: `Insufficient stock for ${product.name} (requested ${qty}, available ${product.stock})`,
          tags: { env: 'production', service: 'shopfast', productId },
          userContext: { userId },
          payload: { requested: qty, available: product.stock },
        });
        void reply.status(409).send({ error: 'Insufficient stock' });
        return;
      }

      pb.capture({
        type: 'custom',
        severity: 'info',
        message: `Item added to cart: ${product.name} ×${qty}`,
        tags: { env: 'production', service: 'shopfast', productId },
        userContext: { userId },
        payload: { productName: product.name, qty, unitPrice: product.price },
      });

      void reply.send({ ok: true, cartTotal: product.price * qty });
    },
  );

  app.post<{ Body: { userId: string; email: string; items: { productId: string; qty: number }[] } }>(
    '/checkout',
    async (req, reply) => {
      const { userId, email, items } = req.body;
      orderCount++;
      const orderId = `ORD-${String(orderCount).padStart(5, '0')}`;

      // Simulate random payment failure (~20%)
      const paymentFailed = Math.random() < 0.2;

      if (paymentFailed) {
        const err = new Error('PaymentGatewayException: card declined by issuer (code: INSUFFICIENT_FUNDS)');
        pb.captureError(err, {
          severity: 'error',
          tags: { env: 'production', service: 'shopfast', component: 'payment' },
          userContext: { userId, email },
          payload: {
            orderId,
            itemCount: items.length,
            gateway: 'stripe',
            declineCode: 'insufficient_funds',
          },
        });
        void reply.status(402).send({ error: 'Payment declined', orderId });
        return;
      }

      // Simulate occasional DB timeout (~5%)
      if (Math.random() < 0.05) {
        const err = new Error('DatabaseTimeoutException: query exceeded 5000ms on orders table');
        pb.captureError(err, {
          severity: 'fatal',
          tags: { env: 'production', service: 'shopfast', component: 'database' },
          userContext: { userId, email },
          payload: { orderId, queryTimeoutMs: 5000, table: 'orders' },
        });
        void reply.status(503).send({ error: 'Service temporarily unavailable' });
        return;
      }

      pb.capture({
        type: 'custom',
        severity: 'info',
        message: `Order placed successfully: ${orderId}`,
        tags: { env: 'production', service: 'shopfast', component: 'checkout' },
        userContext: { userId, email },
        payload: {
          orderId,
          itemCount: items.length,
          total: items.reduce((sum, item) => {
            const p = PRODUCTS.find((pr) => pr.id === item.productId);
            return sum + (p?.price ?? 0) * item.qty;
          }, 0),
        },
      });

      void reply.send({ ok: true, orderId });
    },
  );

  await app.listen({ port, host: '127.0.0.1' });
  return () => app.close();
}

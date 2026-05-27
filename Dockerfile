# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — builder
# Compiles TypeScript to dist/ and installs production dependencies.
# ─────────────────────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies first (layer-cached unless lock file changes)
COPY package.json package-lock.json ./
RUN npm ci

# Copy source and compile
COPY tsconfig.json ./
COPY src ./src
COPY bin ./bin
COPY migrations ./migrations
COPY seed ./seed

RUN npm run build

# Copy non-TS assets that tsc does not emit but the compiled code still reads at runtime.
RUN cp -r src/lib/lua dist/src/lib/lua

# Prune dev dependencies so only production modules are copied to the runner stage
RUN npm prune --omit=dev

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — runner
# Lean production image; only dist/ and node_modules are copied.
# ─────────────────────────────────────────────────────────────────────────────
FROM node:20-alpine AS runner

RUN addgroup -S pulseboard && adduser -S pulseboard -G pulseboard

WORKDIR /app

ENV NODE_ENV=production

COPY --from=builder --chown=pulseboard:pulseboard /app/dist ./dist
COPY --from=builder --chown=pulseboard:pulseboard /app/node_modules ./node_modules
COPY --from=builder --chown=pulseboard:pulseboard /app/package.json ./package.json
COPY --from=builder --chown=pulseboard:pulseboard /app/migrations ./migrations

USER pulseboard

# Default entrypoint is `node`; CMD is overridden per service in docker-compose.yml.
ENTRYPOINT ["node"]
CMD ["dist/bin/api.js"]

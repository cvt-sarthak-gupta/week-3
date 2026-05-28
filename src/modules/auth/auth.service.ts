import type { PostgresPool } from '../../db/postgres/index.js';
import { AuthError, ConflictError, ValidationError } from '../../utils/errors.js';
import { hashPassword, verifyPassword } from '../../utils/crypto.js';
import type { UserRow, RegisterResult } from './auth.types.js';

export class AuthService {
  constructor(private readonly pool: PostgresPool) {}

  async findByEmail(email: string): Promise<UserRow | null> {
    const result = await this.pool.query<UserRow>(
      `SELECT u.id, u.email, u.full_name, u.password_hash, tm.tenant_id, tm.role
       FROM users u LEFT JOIN tenant_members tm ON tm.user_id = u.id
       WHERE u.email = $1 LIMIT 1`,
      [email],
    );
    return result.rows[0] ?? null;
  }

  async register(email: string, password: string, fullName: string): Promise<RegisterResult> {
    const existing = await this.pool.query<{ id: string }>(
      'SELECT id FROM users WHERE email = $1 LIMIT 1',
      [email],
    );
    if ((existing.rowCount ?? 0) > 0) {
      throw new ConflictError('Email already registered');
    }
    if (password.length < 8) {
      throw new ValidationError('Password must be at least 8 characters');
    }
    const passwordHash = await hashPassword(password);
    const result = await this.pool.query<{ id: string }>(
      'INSERT INTO users (email, password_hash, full_name) VALUES ($1, $2, $3) RETURNING id',
      [email, passwordHash, fullName],
    );
    const userId = result.rows[0]?.id;
    if (!userId) {
      throw new Error('Failed to create user');
    }
    return { userId, email };
  }

  async verifyCredentials(email: string, password: string): Promise<UserRow> {
    const user = await this.findByEmail(email);
    if (!user) {
      throw new AuthError('Invalid email or password');
    }
    const valid = await verifyPassword(password, user.password_hash);
    if (!valid) {
      throw new AuthError('Invalid email or password');
    }
    return user;
  }

  async updateLastLogin(userId: string): Promise<void> {
    await this.pool.query('UPDATE users SET last_login_at = NOW() WHERE id = $1', [userId]);
  }
}

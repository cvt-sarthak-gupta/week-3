export interface RegisterBody { email: string; password: string; fullName: string }
export interface LoginBody { email: string; password: string }
export interface RefreshBody { refreshToken: string }
export interface RegisterResult { userId: string; email: string }
export interface LoginResult {
  accessToken: string;
  refreshToken: string;
  user: { id: string; email: string; fullName: string };
}
interface UserRow { id: string; email: string; full_name: string; password_hash: string; tenant_id: string | null; role: string }
export type { UserRow };

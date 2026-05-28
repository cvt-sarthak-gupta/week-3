export interface ProjectRow {
  id: string;
  tenant_id: string;
  name: string;
  slug: string;
  api_key: string;
  is_archived: boolean;
  created_at: Date;
  updated_at: Date;
}

export interface CreateProjectInput {
  name: string;
  slug: string;
}

export interface RollKeyResult {
  apiKey: string;
}

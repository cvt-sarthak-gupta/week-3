export interface LeaderboardEntry {
  projectId: string;
  projectName: string;
  eventCount: number;
  rank: number;
}

export interface LeaderboardResult {
  date: string;
  leaderboard: LeaderboardEntry[];
}

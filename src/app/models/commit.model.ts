export interface GitCommit {
  sha: string;
  shortSha: string;
  message: string;
  author: string;
  authorLogin: string | null;
  avatarUrl: string | null;
  date: Date;
  url: string;
}

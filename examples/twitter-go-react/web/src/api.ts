const API_BASE = '';

function getToken(): string | null {
  return localStorage.getItem('chirp_token');
}

function authHeaders(): Record<string, string> {
  const token = getToken();
  if (token) return { Authorization: `Bearer ${token}` };
  return {};
}

async function request<T>(url: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...authHeaders(),
    ...(options.headers as Record<string, string> || {}),
  };
  const res = await fetch(API_BASE + url, { ...options, headers });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error || `Request failed: ${res.status}`);
  return json;
}

export interface User {
  id: string;
  handle: string;
  displayName: string;
  bio?: string;
  avatarUrl?: string;
  headerUrl?: string;
  location?: string;
  website?: string;
  createdAt: number;
}

export interface UserProfile extends User {
  followerCount: number;
  followingCount: number;
  tweetCount: number;
  isFollowing: boolean;
}

export interface Tweet {
  id: string;
  authorId: string;
  content: string;
  replyToId?: string;
  quoteOfId?: string;
  replyCount: number;
  retweetCount: number;
  likeCount: number;
  quoteCount: number;
  createdAt: number;
  author?: User;
  quotedTweet?: Tweet;
  liked: boolean;
  retweeted: boolean;
}

export interface Notification {
  id: string;
  userId: string;
  actorId: string;
  type: 'like' | 'retweet' | 'reply' | 'follow' | 'quote';
  tweetId?: string;
  read: boolean;
  createdAt: number;
  actor?: User;
}

// Auth
export async function register(data: { email: string; password: string; handle: string; displayName: string }) {
  return request<{ ok: boolean; token: string; user: User }>('/api/auth/register', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function login(data: { email: string; password: string }) {
  return request<{ ok: boolean; token: string; user: User }>('/api/auth/login', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function getMe() {
  return request<{ ok: boolean; user: User }>('/api/auth/me');
}

// Timeline
export async function getTimeline(limit = 50, offset = 0) {
  return request<{ ok: boolean; data: Tweet[] }>(`/api/timeline?limit=${limit}&offset=${offset}`);
}

// Tweets
export async function createTweet(data: { content: string; replyToId?: string; quoteOfId?: string }) {
  return request<{ ok: boolean; data: Tweet }>('/api/tweets', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function getTweet(id: string) {
  return request<{ ok: boolean; data: Tweet }>(`/api/tweets/${id}`);
}

export async function getTweetReplies(id: string, limit = 50, offset = 0) {
  return request<{ ok: boolean; data: Tweet[] }>(`/api/tweets/${id}/replies?limit=${limit}&offset=${offset}`);
}

// Likes & Retweets
export async function toggleLike(tweetId: string) {
  return request<{ ok: boolean; liked: boolean }>(`/api/like/${tweetId}`, { method: 'POST' });
}

export async function toggleRetweet(tweetId: string) {
  return request<{ ok: boolean; retweeted: boolean }>(`/api/retweet/${tweetId}`, { method: 'POST' });
}

// Follows
export async function toggleFollow(userId: string) {
  return request<{ ok: boolean; following: boolean }>(`/api/follow/${userId}`, { method: 'POST' });
}

// Users
export async function getUserProfile(handle: string) {
  return request<{ ok: boolean; data: UserProfile }>(`/api/users/${handle}`);
}

export async function getUserTweets(handle: string, limit = 50, offset = 0) {
  return request<{ ok: boolean; data: Tweet[] }>(`/api/users/${handle}/tweets?limit=${limit}&offset=${offset}`);
}

// Search
export async function search(q: string, type?: 'tweets' | 'users', limit = 20) {
  const params = new URLSearchParams({ q, limit: String(limit) });
  if (type) params.set('type', type);
  return request<{ ok: boolean; tweets?: Tweet[]; users?: User[] }>(`/api/search?${params}`);
}

// Notifications
export async function getNotifications(limit = 50, offset = 0) {
  return request<{ ok: boolean; data: Notification[] }>(`/api/notifications?limit=${limit}&offset=${offset}`);
}

// Stats
export async function getStats() {
  return request<{ ok: boolean; data: Record<string, number> }>('/api/stats');
}

// Token management
export function setToken(token: string) {
  localStorage.setItem('chirp_token', token);
}

export function clearToken() {
  localStorage.removeItem('chirp_token');
}

export function hasToken(): boolean {
  return !!localStorage.getItem('chirp_token');
}

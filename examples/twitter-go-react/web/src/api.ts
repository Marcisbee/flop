import { Flop, type TokenStore } from './flop-sdk';

const TOKEN_KEY = 'chirp_token';

const tokenStore: TokenStore = {
  get() {
    return localStorage.getItem(TOKEN_KEY);
  },
  set(token: string) {
    localStorage.setItem(TOKEN_KEY, token);
  },
  clear() {
    localStorage.removeItem(TOKEN_KEY);
  },
};

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

type TwitterSchema = {
  views: {
    auth_me: { input: {}; output: User };
    get_timeline: { input: { limit?: number; offset?: number }; output: Tweet[] };
    get_tweet: { input: { tweetId: string }; output: Tweet };
    get_tweet_replies: { input: { tweetId: string; limit?: number; offset?: number }; output: Tweet[] };
    get_user_profile: { input: { handle: string }; output: UserProfile };
    get_user_tweets: { input: { handle: string; limit?: number; offset?: number }; output: Tweet[] };
    search: { input: { q: string; type?: 'tweets' | 'users'; limit?: number }; output: { tweets?: Tweet[]; users?: User[] } };
    get_notifications: { input: { limit?: number; offset?: number }; output: Notification[] };
    get_stats: { input: {}; output: Record<string, number> };
  };
  reducers: {
    auth_register: { input: { email: string; password: string; handle: string; displayName: string }; output: { token: string; user: User } };
    auth_login: { input: { email: string; password: string }; output: { token: string; user: User } };
    create_tweet: { input: { content: string; replyToId?: string; quoteOfId?: string }; output: Tweet };
    toggle_like: { input: { tweetId: string }; output: { liked: boolean } };
    toggle_retweet: { input: { tweetId: string }; output: { retweeted: boolean } };
    toggle_follow: { input: { userId: string }; output: { following: boolean } };
  };
};

const client = new Flop<TwitterSchema>({
  host: '',
  tokenStore,
  batchViews: 'frame',
  autoRefetch: true,
});

// Auth
export async function register(data: { email: string; password: string; handle: string; displayName: string }) {
  const out = await client.reducer('auth_register', data);
  tokenStore.set(out.token);
  return { ok: true, token: out.token, user: out.user };
}

export async function login(data: { email: string; password: string }) {
  const out = await client.reducer('auth_login', data);
  tokenStore.set(out.token);
  return { ok: true, token: out.token, user: out.user };
}

export async function getMe() {
  const user = await client.view('auth_me', {});
  return { ok: true, user };
}

// Timeline
export async function getTimeline(limit = 50, offset = 0) {
  const data = await client.view('get_timeline', { limit, offset });
  return { ok: true, data };
}

// Tweets
export async function createTweet(data: { content: string; replyToId?: string; quoteOfId?: string }) {
  const row = await client.reducer('create_tweet', data);
  return { ok: true, data: row };
}

export async function getTweet(id: string) {
  const data = await client.view('get_tweet', { tweetId: id });
  return { ok: true, data };
}

export async function getTweetReplies(id: string, limit = 50, offset = 0) {
  const data = await client.view('get_tweet_replies', { tweetId: id, limit, offset });
  return { ok: true, data };
}

// Likes & Retweets
export async function toggleLike(tweetId: string) {
  const data = await client.reducer('toggle_like', { tweetId });
  return { ok: true, liked: !!data?.liked };
}

export async function toggleRetweet(tweetId: string) {
  const data = await client.reducer('toggle_retweet', { tweetId });
  return { ok: true, retweeted: !!data?.retweeted };
}

// Follows
export async function toggleFollow(userId: string) {
  const data = await client.reducer('toggle_follow', { userId });
  return { ok: true, following: !!data?.following };
}

// Users
export async function getUserProfile(handle: string) {
  const data = await client.view('get_user_profile', { handle });
  return { ok: true, data };
}

export async function getUserTweets(handle: string, limit = 50, offset = 0) {
  const data = await client.view('get_user_tweets', { handle, limit, offset });
  return { ok: true, data };
}

// Search
export async function search(q: string, type?: 'tweets' | 'users', limit = 20) {
  const data = await client.view('search', { q, type, limit });
  return { ok: true, tweets: data?.tweets, users: data?.users };
}

// Notifications
export async function getNotifications(limit = 50, offset = 0) {
  const data = await client.view('get_notifications', { limit, offset });
  return { ok: true, data };
}

// Stats
export async function getStats() {
  const data = await client.view('get_stats', {});
  return { ok: true, data };
}

// Token management
export function setToken(token: string) {
  tokenStore.set(token);
}

export function clearToken() {
  tokenStore.clear();
}

export function hasToken(): boolean {
  return !!tokenStore.get();
}

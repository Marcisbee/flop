// Lightweight typed API client for the Zig server

const TOKEN_KEY = 'reddit_token';

function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

function setToken(token: string) {
  localStorage.setItem(TOKEN_KEY, token);
}

function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

function hasToken(): boolean {
  return !!localStorage.getItem(TOKEN_KEY);
}

async function fetchAPI<T>(path: string, opts?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {};
  if (!opts || !(opts.body instanceof FormData)) {
    headers['Content-Type'] = 'application/json';
  }
  const token = getToken();
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  const res = await fetch(path, {
    ...opts,
    headers: { ...headers, ...opts?.headers },
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || res.statusText);
  }
  return res.json();
}

function get<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  let url = path;
  if (params) {
    const qs = Object.entries(params)
      .filter(([, v]) => v !== undefined && v !== '')
      .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`)
      .join('&');
    if (qs) url += '?' + qs;
  }
  return fetchAPI<T>(url);
}

function post<T>(path: string, body?: any): Promise<T> {
  return fetchAPI<T>(path, { method: 'POST', body: JSON.stringify(body) });
}

// ===== Raw Row shape =====

interface RawRow {
  ID: number;
  TableID: number;
  Data: Record<string, any>;
  CreatedAt: string;
  UpdatedAt: string;
  Version: number;
}

// Flatten a raw row into a simpler object: {id, created_at, updated_at, ...Data}
function flattenRow(raw: RawRow): any {
  const out: any = { id: raw.ID, ...raw.Data };
  out.created_at = new Date(raw.CreatedAt).getTime();
  out.updated_at = new Date(raw.UpdatedAt).getTime();
  // Flatten ref includes: _ref_author_id -> _author_id
  for (const key of Object.keys(out)) {
    if (key.startsWith('_ref_')) {
      const shortKey = '_' + key.slice(5);
      const refData = out[key];
      if (refData && typeof refData === 'object') {
        out[shortKey] = { id: refData.id, ...refData };
      }
      delete out[key];
    }
  }
  return out;
}

interface RawViewResponse {
  data: RawRow[] | null;
  total: number;
}

async function getView<T>(path: string, params?: Record<string, string | number | undefined>): Promise<{ data: T[]; total: number }> {
  const raw = await get<RawViewResponse>(path, params);
  return {
    data: (raw.data || []).map(r => flattenRow(r) as T),
    total: raw.total,
  };
}

// ===== Types =====

export interface User {
  id: number;
  email?: string;
  handle: string;
  display_name: string;
  bio?: string;
  avatar?: string;
  karma: number;
}

export interface Community {
  id: number;
  name: string;
  handle: string;
  description?: string;
  rules?: string;
  creator_id: number;
  member_count: number;
  visibility: string;
  _creator_id?: User;
}

export interface Post {
  id: number;
  title: string;
  body?: string;
  link?: string;
  image?: string;
  author_id: number;
  community_id: number;
  score: number;
  hot_rank: number;
  comment_count: number;
  repost_of?: number;
  created_at: number;
  updated_at: number;
  _author_id?: User;
  _community_id?: Community;
}

export interface Comment {
  id: number;
  body: string;
  author_id: number;
  post_id: number;
  parent_id?: number;
  depth: number;
  path: string;
  score: number;
  created_at: number;
  _author_id?: User;
}

// ===== Auth =====

export async function register(email: string, password: string, handle: string, displayName: string) {
  const res = await post<{ user: User; token: string }>('/api/auth/register', {
    email, password, handle, display_name: displayName,
  });
  setToken(res.token);
  return res;
}

export async function login(email: string, password: string) {
  const res = await post<{ user: User; token: string }>('/api/auth/login', { email, password });
  setToken(res.token);
  return res;
}

export async function getMe() {
  return get<{ user: User }>('/api/auth/me');
}

export { hasToken, clearToken };

// ===== Communities =====

export async function getCommunities() {
  return getView<Community>('/api/communities');
}

export async function getCommunity(handle: string) {
  return getView<Community>(`/api/communities/${handle}`);
}

export async function createCommunity(name: string, handle: string, description?: string) {
  return post<any>('/api/communities', { name, handle, description });
}

export async function toggleJoinCommunity(communityId: number) {
  return post<{ joined: boolean }>(`/api/communities/${communityId}/toggle_join`, {});
}

export async function checkMembership(communityId: number) {
  return get<{ joined: boolean }>(`/api/communities/${communityId}/membership`);
}

// ===== Posts =====

export async function getHotFeed(limit?: number, offset?: number) {
  return getView<Post>('/api/feed/hot', { limit, offset });
}

export async function getNewFeed(limit?: number, offset?: number) {
  return getView<Post>('/api/feed/new', { limit, offset });
}

export async function getBestFeed(limit?: number, offset?: number) {
  return getView<Post>('/api/feed/best', { limit, offset });
}

export async function getCommunityPosts(communityId: number, limit?: number, offset?: number) {
  return getView<Post>(`/api/c/${communityId}/posts`, { limit, offset });
}

export async function getPost(id: number) {
  return getView<Post>(`/api/posts/${id}`);
}

export async function getUserPosts(authorId: number, limit?: number, offset?: number) {
  return getView<Post>(`/api/users/${authorId}/posts`, { limit, offset });
}

export async function searchPosts(q: string, limit?: number) {
  return getView<Post>('/api/search/posts', { q, limit });
}

export async function createPost(title: string, communityId: number, body?: string, link?: string) {
  return post<any>('/api/posts', {
    title, community_id: communityId, body, link,
  });
}

// ===== Votes =====

export async function votePost(postId: number, value: number) {
  return post<{ score: number; user_vote: number }>(`/api/posts/${postId}/vote`, { value });
}

export async function voteComment(commentId: number, value: number) {
  return post<{ score: number; user_vote: number }>(`/api/comments/${commentId}/vote`, { value });
}

// ===== Comments =====

export async function getComments(postId: number) {
  return getView<Comment>(`/api/posts/${postId}/comments`);
}

export async function createComment(postId: number, body: string, parentId?: number) {
  return post<{ comment: any }>(`/api/posts/${postId}/comments`, {
    body, parent_id: parentId || 0,
  });
}

// ===== Repost =====

export async function repost(postId: number, communityId?: number, title?: string) {
  return post<{ post: any }>(`/api/posts/${postId}/repost`, {
    community_id: communityId, title,
  });
}

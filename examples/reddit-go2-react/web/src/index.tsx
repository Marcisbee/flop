import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { createRoot } from 'react-dom/client';
import * as api from './api';
import type { User, Post, Community, Comment } from './api';
import { PostCard, CommentItem, CreatePostForm, CommunityCard, Spinner, EmptyState } from './components';
import { HomeIcon, SearchIcon, UserIcon, PlusIcon, ArrowLeftIcon, XIcon, FireIcon, ClockIcon, TrophyIcon, UsersIcon, LogoIcon } from './icons';
import './app.css';

// ===== Router =====

function useRouter() {
  const [path, setPath] = useState(window.location.pathname);

  useEffect(() => {
    const onPop = () => setPath(window.location.pathname);
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);

  const navigate = useCallback((to: string) => {
    window.history.pushState(null, '', to);
    setPath(to);
    window.scrollTo(0, 0);
  }, []);

  return { path, navigate };
}

// ===== Auth Hook =====

function useAuth() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!api.hasToken()) {
      setLoading(false);
      return;
    }
    api.getMe()
      .then(res => setUser(res.user))
      .catch(() => api.clearToken())
      .finally(() => setLoading(false));
  }, []);

  const loginUser = useCallback(async (email: string, password: string) => {
    const res = await api.login(email, password);
    setUser(res.user);
    return res;
  }, []);

  const registerUser = useCallback(async (email: string, password: string, handle: string, displayName: string) => {
    const res = await api.register(email, password, handle, displayName);
    setUser(res.user);
    return res;
  }, []);

  const logout = useCallback(() => {
    api.clearToken();
    setUser(null);
  }, []);

  return { user, loading, loginUser, registerUser, logout };
}

// ===== Pages =====

function HomePage({
  navigate,
  user,
}: {
  navigate: (to: string) => void;
  user: User | null;
}) {
  const [posts, setPosts] = useState<Post[]>([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState<'hot' | 'new' | 'best'>('hot');
  const [communities, setCommunities] = useState<Community[]>([]);

  useEffect(() => {
    api.getCommunities().then(r => setCommunities(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    const fetchFn = tab === 'hot' ? api.getHotFeed : tab === 'new' ? api.getNewFeed : api.getBestFeed;
    fetchFn()
      .then(res => setPosts(res.data))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [tab]);

  return (
    <>
      {user && communities.length > 0 && (
        <CreatePostForm communities={communities} onCreated={() => {
          const fetchFn = tab === 'hot' ? api.getHotFeed : tab === 'new' ? api.getNewFeed : api.getBestFeed;
          fetchFn().then(res => setPosts(res.data)).catch(() => {});
        }} />
      )}
      <div className="feed-tabs">
        <button className={tab === 'hot' ? 'active' : ''} onClick={() => setTab('hot')}>
          <FireIcon /> Hot
        </button>
        <button className={tab === 'new' ? 'active' : ''} onClick={() => setTab('new')}>
          <ClockIcon /> New
        </button>
        <button className={tab === 'best' ? 'active' : ''} onClick={() => setTab('best')}>
          <TrophyIcon /> Best
        </button>
      </div>
      {loading ? <Spinner /> : posts.length === 0 ? (
        <EmptyState title="No posts yet" message="Be the first to post something!" />
      ) : (
        <div className="post-list">
          {posts.map(p => (
            <PostCard
              key={p.id}
              post={p}
              onClick={id => navigate(`/post/${id}`)}
              onCommunityClick={h => navigate(`/r/${h}`)}
              onUserClick={id => navigate(`/u/${id}`)}
            />
          ))}
        </div>
      )}
    </>
  );
}

function PostDetailPage({
  postId,
  navigate,
  user,
}: {
  postId: number;
  navigate: (to: string) => void;
  user: User | null;
}) {
  const [post, setPost] = useState<Post | null>(null);
  const [comments, setComments] = useState<Comment[]>([]);
  const [loading, setLoading] = useState(true);
  const [replyTo, setReplyTo] = useState<number | undefined>(undefined);
  const [replyBody, setReplyBody] = useState('');
  const [posting, setPosting] = useState(false);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.getPost(postId).then(r => setPost(r.data[0] || null)),
      api.getComments(postId).then(r => setComments(r.data)),
    ]).finally(() => setLoading(false));
  }, [postId]);

  const handleComment = useCallback(async () => {
    if (!replyBody.trim() || posting) return;
    setPosting(true);
    try {
      await api.createComment(postId, replyBody.trim(), replyTo);
      setReplyBody('');
      setReplyTo(undefined);
      const r = await api.getComments(postId);
      setComments(r.data);
      // Refresh post to get updated comment count
      api.getPost(postId).then(r => setPost(r.data[0] || null));
    } catch (err) {
      console.error(err);
    } finally {
      setPosting(false);
    }
  }, [postId, replyBody, replyTo, posting]);

  if (loading) return <Spinner />;
  if (!post) return <EmptyState title="Not found" message="This post doesn't exist." />;

  return (
    <div className="post-detail">
      <button className="back-btn" onClick={() => window.history.back()}>
        <ArrowLeftIcon /> Back
      </button>
      <PostCard
        post={post}
        showCommunity
        onCommunityClick={h => navigate(`/r/${h}`)}
        onUserClick={id => navigate(`/u/${id}`)}
      />
      {user && (
        <div className="comment-form">
          {replyTo && (
            <div className="reply-indicator">
              Replying to comment
              <button onClick={() => setReplyTo(undefined)}><XIcon /></button>
            </div>
          )}
          <textarea
            placeholder={replyTo ? 'Write a reply...' : 'Add a comment...'}
            value={replyBody}
            onChange={e => setReplyBody(e.target.value)}
            rows={3}
          />
          <button className="btn-primary" onClick={handleComment} disabled={!replyBody.trim() || posting}>
            {posting ? 'Posting...' : replyTo ? 'Reply' : 'Comment'}
          </button>
        </div>
      )}
      <div className="comments-section">
        <h3>{post.comment_count} Comments</h3>
        {comments.length === 0 ? (
          <EmptyState title="No comments" message="Be the first to comment!" />
        ) : (
          comments.map(c => (
            <CommentItem
              key={c.id}
              comment={c}
              onReply={(parentId) => { setReplyTo(parentId); }}
              user={user}
            />
          ))
        )}
      </div>
    </div>
  );
}

function CommunityPage({
  handle,
  navigate,
  user,
}: {
  handle: string;
  navigate: (to: string) => void;
  user: User | null;
}) {
  const [community, setCommunity] = useState<Community | null>(null);
  const [posts, setPosts] = useState<Post[]>([]);
  const [loading, setLoading] = useState(true);
  const [joined, setJoined] = useState(false);

  useEffect(() => {
    setLoading(true);
    api.getCommunity(handle)
      .then(r => {
        const c = r.data[0];
        setCommunity(c || null);
        if (c) {
          const promises: Promise<any>[] = [
            api.getCommunityPosts(c.id).then(r => setPosts(r.data)),
          ];
          if (user) {
            promises.push(
              api.checkMembership(c.id).then(r => setJoined(r.joined))
            );
          }
          return Promise.all(promises);
        }
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [handle, user]);

  const handleToggleJoin = useCallback(async () => {
    if (!community) return;
    try {
      const res = await api.toggleJoinCommunity(community.id);
      setJoined(res.joined);
      setCommunity(prev => prev ? {
        ...prev,
        member_count: prev.member_count + (res.joined ? 1 : -1),
      } : null);
    } catch (err) {
      console.error(err);
    }
  }, [community]);

  if (loading) return <Spinner />;
  if (!community) return <EmptyState title="Not found" message="This community doesn't exist." />;

  return (
    <div>
      <div className="community-header">
        <div className="community-banner">
          <h1>r/{community.handle}</h1>
          <p>{community.name}</p>
        </div>
        <div className="community-info">
          <p>{community.description}</p>
          <div className="community-stats">
            <span><UsersIcon /> {community.member_count} members</span>
          </div>
          {user && (
            <button
              className={`btn-primary ${joined ? 'btn-joined' : ''}`}
              onClick={handleToggleJoin}
            >
              {joined ? 'Joined' : 'Join'}
            </button>
          )}
        </div>
      </div>
      {user && community && (
        <CreatePostForm communities={[community]} onCreated={() => {
          api.getCommunityPosts(community.id).then(r => setPosts(r.data));
        }} />
      )}
      <div className="post-list">
        {posts.length === 0 ? (
          <EmptyState title="No posts" message="Be the first to post in this community!" />
        ) : (
          posts.map(p => (
            <PostCard
              key={p.id}
              post={p}
              showCommunity={false}
              onClick={id => navigate(`/post/${id}`)}
              onUserClick={id => navigate(`/u/${id}`)}
            />
          ))
        )}
      </div>
    </div>
  );
}

function CommunitiesPage({
  navigate,
  user,
}: {
  navigate: (to: string) => void;
  user: User | null;
}) {
  const [communities, setCommunities] = useState<Community[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState('');
  const [handle, setHandle] = useState('');
  const [desc, setDesc] = useState('');

  useEffect(() => {
    api.getCommunities()
      .then(r => setCommunities(r.data))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const handleCreate = useCallback(async () => {
    if (!name.trim() || !handle.trim()) return;
    try {
      await api.createCommunity(name.trim(), handle.trim(), desc.trim());
      setShowCreate(false);
      setName(''); setHandle(''); setDesc('');
      api.getCommunities().then(r => setCommunities(r.data));
    } catch (err) {
      console.error(err);
    }
  }, [name, handle, desc]);

  if (loading) return <Spinner />;

  return (
    <div>
      <div className="page-header">
        <h2>Communities</h2>
        {user && (
          <button className="btn-primary" onClick={() => setShowCreate(!showCreate)}>
            <PlusIcon /> Create
          </button>
        )}
      </div>
      {showCreate && (
        <div className="create-community-form">
          <input placeholder="Community name" value={name} onChange={e => setName(e.target.value)} />
          <input placeholder="Handle (e.g. programming)" value={handle} onChange={e => setHandle(e.target.value)} />
          <textarea placeholder="Description" value={desc} onChange={e => setDesc(e.target.value)} rows={3} />
          <button className="btn-primary" onClick={handleCreate} disabled={!name.trim() || !handle.trim()}>
            Create Community
          </button>
        </div>
      )}
      <div className="community-list">
        {communities.map(c => (
          <CommunityCard key={c.id} community={c} onClick={h => navigate(`/r/${h}`)} />
        ))}
      </div>
    </div>
  );
}

function SearchPage({ navigate }: { navigate: (to: string) => void }) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<Post[]>([]);
  const [searched, setSearched] = useState(false);

  const handleSearch = useCallback(async () => {
    if (!query.trim()) return;
    setSearched(true);
    try {
      const res = await api.searchPosts(query.trim());
      setResults(res.data);
    } catch (err) {
      console.error(err);
    }
  }, [query]);

  return (
    <div>
      <h2>Search</h2>
      <div className="search-bar">
        <input
          type="text"
          placeholder="Search posts..."
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && handleSearch()}
        />
        <button className="btn-primary" onClick={handleSearch}>
          <SearchIcon />
        </button>
      </div>
      {searched && results.length === 0 ? (
        <EmptyState title="No results" message={`No posts found for "${query}"`} />
      ) : (
        <div className="post-list">
          {results.map(p => (
            <PostCard
              key={p.id}
              post={p}
              onClick={id => navigate(`/post/${id}`)}
              onCommunityClick={h => navigate(`/r/${h}`)}
              onUserClick={id => navigate(`/u/${id}`)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function UserPage({
  userId,
  navigate,
}: {
  userId: number;
  navigate: (to: string) => void;
}) {
  const [posts, setPosts] = useState<Post[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.getUserPosts(userId)
      .then(r => setPosts(r.data))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [userId]);

  if (loading) return <Spinner />;

  return (
    <div>
      <div className="page-header">
        <h2>User Posts</h2>
      </div>
      {posts.length === 0 ? (
        <EmptyState title="No posts" message="This user hasn't posted anything yet." />
      ) : (
        <div className="post-list">
          {posts.map(p => (
            <PostCard
              key={p.id}
              post={p}
              onClick={id => navigate(`/post/${id}`)}
              onCommunityClick={h => navigate(`/r/${h}`)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function AuthPage({
  mode,
  navigate,
  loginUser,
  registerUser,
}: {
  mode: 'login' | 'register';
  navigate: (to: string) => void;
  loginUser: (email: string, password: string) => Promise<any>;
  registerUser: (email: string, password: string, handle: string, displayName: string) => Promise<any>;
}) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [handle, setHandle] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [error, setError] = useState('');

  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    try {
      if (mode === 'login') {
        await loginUser(email, password);
      } else {
        await registerUser(email, password, handle, displayName);
      }
      navigate('/');
    } catch (err: any) {
      setError(err.message);
    }
  }, [mode, email, password, handle, displayName, loginUser, registerUser, navigate]);

  return (
    <div className="auth-page">
      <div className="auth-card">
        <h2>{mode === 'login' ? 'Sign In' : 'Create Account'}</h2>
        {error && <div className="error-msg">{error}</div>}
        <form onSubmit={handleSubmit}>
          {mode === 'register' && (
            <>
              <input type="text" placeholder="Handle (e.g. cooluser)" value={handle} onChange={e => setHandle(e.target.value)} required />
              <input type="text" placeholder="Display Name" value={displayName} onChange={e => setDisplayName(e.target.value)} />
            </>
          )}
          <input type="email" placeholder="Email" value={email} onChange={e => setEmail(e.target.value)} required />
          <input type="password" placeholder="Password" value={password} onChange={e => setPassword(e.target.value)} required />
          <button type="submit" className="btn-primary">
            {mode === 'login' ? 'Sign In' : 'Sign Up'}
          </button>
        </form>
        <p className="auth-switch">
          {mode === 'login' ? (
            <>Don't have an account? <a onClick={() => navigate('/register')}>Sign up</a></>
          ) : (
            <>Already have an account? <a onClick={() => navigate('/login')}>Sign in</a></>
          )}
        </p>
      </div>
    </div>
  );
}

// ===== App =====

function App() {
  const { path, navigate } = useRouter();
  const { user, loading, loginUser, registerUser, logout } = useAuth();

  const route = useMemo(() => {
    if (path === '/' || path === '/home') return { page: 'home' as const };
    if (path === '/communities') return { page: 'communities' as const };
    if (path === '/search') return { page: 'search' as const };
    if (path === '/login') return { page: 'auth' as const, mode: 'login' as const };
    if (path === '/register') return { page: 'auth' as const, mode: 'register' as const };
    if (path.startsWith('/post/')) return { page: 'post' as const, id: Number(path.slice(6)) };
    if (path.startsWith('/r/')) return { page: 'community' as const, handle: path.slice(3) };
    if (path.startsWith('/u/')) return { page: 'user' as const, id: Number(path.slice(3)) };
    return { page: 'home' as const };
  }, [path]);

  if (loading) return <div className="app-loading"><Spinner /></div>;

  return (
    <div className="app">
      {/* Sidebar */}
      <nav className="sidebar">
        <div className="sidebar-logo" onClick={() => navigate('/')}>
          <LogoIcon className="icon-logo" />
          <span className="logo-text">leddit</span>
        </div>
        <div className="nav-items">
          <a className={`nav-item ${route.page === 'home' ? 'active' : ''}`} onClick={() => navigate('/')}>
            <HomeIcon /> <span>Home</span>
          </a>
          <a className={`nav-item ${route.page === 'communities' ? 'active' : ''}`} onClick={() => navigate('/communities')}>
            <UsersIcon /> <span>Communities</span>
          </a>
          <a className={`nav-item ${route.page === 'search' ? 'active' : ''}`} onClick={() => navigate('/search')}>
            <SearchIcon /> <span>Search</span>
          </a>
        </div>
        <div className="sidebar-bottom">
          {user ? (
            <div className="user-card">
              <div className="user-avatar">{(user.display_name || user.handle).charAt(0).toUpperCase()}</div>
              <div className="user-info">
                <div className="user-name">{user.display_name || user.handle}</div>
                <div className="user-handle">u/{user.handle}</div>
              </div>
              <button className="logout-btn" onClick={logout}>Log out</button>
            </div>
          ) : (
            <button className="btn-primary login-btn" onClick={() => navigate('/login')}>
              Sign In
            </button>
          )}
        </div>
      </nav>

      {/* Main content */}
      <main className="main-content">
        {route.page === 'home' && <HomePage navigate={navigate} user={user} />}
        {route.page === 'post' && <PostDetailPage postId={route.id} navigate={navigate} user={user} />}
        {route.page === 'community' && <CommunityPage handle={route.handle} navigate={navigate} user={user} />}
        {route.page === 'communities' && <CommunitiesPage navigate={navigate} user={user} />}
        {route.page === 'search' && <SearchPage navigate={navigate} />}
        {route.page === 'user' && <UserPage userId={route.id} navigate={navigate} />}
        {route.page === 'auth' && (
          <AuthPage mode={route.mode} navigate={navigate} loginUser={loginUser} registerUser={registerUser} />
        )}
      </main>

      {/* Right sidebar */}
      <aside className="right-sidebar">
        <div className="sidebar-widget">
          <h4>About Leddit</h4>
          <p>A Reddit clone built with Flop go2 database engine and React.</p>
        </div>
        <div className="sidebar-widget">
          <h4>Powered by</h4>
          <p>Flop go2 embedded database</p>
        </div>
      </aside>
    </div>
  );
}

// ===== Mount =====

const root = createRoot(document.getElementById('app')!);
root.render(<App />);

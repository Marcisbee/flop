import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { createRoot } from 'react-dom/client';
import type { User, Tweet, UserProfile as UserProfileType, Notification as NotifType } from './api';
import * as api from './api';
import { TweetCard, ComposeBox, Avatar, Spinner, EmptyState } from './components';
import {
  HomeIcon, SearchIcon, BellIcon, UserIcon, FeatherIcon, LogoIcon,
  ArrowLeftIcon, MapPinIcon, LinkIcon, CalendarIcon, XIcon,
  HeartIcon, RepeatIcon, MessageIcon, QuoteIcon,
} from './icons';
import { timeAgo, formatNumber, formatDate } from './utils';

// ---- Simple client-side router ----
function useRouter() {
  const [path, setPath] = useState(window.location.pathname);

  useEffect(() => {
    const onPop = () => setPath(window.location.pathname);
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);

  const navigate = useCallback((to: string) => {
    if (to !== window.location.pathname) {
      window.history.pushState(null, '', to);
      setPath(to);
      window.scrollTo(0, 0);
    }
  }, []);

  return { path, navigate };
}

// ---- Auth Context ----
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
      .catch(() => { api.clearToken(); })
      .finally(() => setLoading(false));
  }, []);

  const loginUser = useCallback(async (email: string, password: string) => {
    const res = await api.login({ email, password });
    api.setToken(res.token);
    setUser(res.user);
    return res.user;
  }, []);

  const registerUser = useCallback(async (data: { email: string; password: string; handle: string; displayName: string }) => {
    const res = await api.register(data);
    api.setToken(res.token);
    setUser(res.user);
    return res.user;
  }, []);

  const logout = useCallback(() => {
    api.clearToken();
    setUser(null);
  }, []);

  return { user, loading, loginUser, registerUser, logout };
}

// ---- Pages ----

function HomePage({ user, navigate }: { user: User | null; navigate: (to: string) => void }) {
  const [tweets, setTweets] = useState<Tweet[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getTimeline(50, 0)
      .then(res => setTweets(res.data || []))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const handleTweetCreated = useCallback((tweet: Tweet) => {
    setTweets(prev => [tweet, ...prev]);
  }, []);

  return (
    <>
      <div className="page-header">
        <h1>Home</h1>
      </div>
      {user && (
        <ComposeBox user={user} onTweetCreated={handleTweetCreated} />
      )}
      {loading ? (
        <Spinner />
      ) : tweets.length === 0 ? (
        <EmptyState title="No tweets yet" message="When people post, their tweets will show up here." />
      ) : (
        tweets.map(t => (
          <TweetCard
            key={t.id}
            tweet={t}
            onClick={(id) => navigate(`/tweet/${id}`)}
            onNavigateProfile={(h) => navigate(`/${h}`)}
          />
        ))
      )}
    </>
  );
}

function TweetDetailPage({ tweetId, user, navigate }: { tweetId: string; user: User | null; navigate: (to: string) => void }) {
  const [tweet, setTweet] = useState<Tweet | null>(null);
  const [replies, setReplies] = useState<Tweet[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.getTweet(tweetId).then(r => setTweet(r.data)),
      api.getTweetReplies(tweetId).then(r => setReplies(r.data || [])),
    ])
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [tweetId]);

  const handleReplyCreated = useCallback((reply: Tweet) => {
    setReplies(prev => [reply, ...prev]);
    if (tweet) {
      setTweet({ ...tweet, replyCount: tweet.replyCount + 1 });
    }
  }, [tweet]);

  if (loading) return <Spinner />;
  if (!tweet) return <EmptyState title="Tweet not found" message="This tweet may have been deleted." />;

  return (
    <>
      <div className="page-header">
        <div className="page-header-back">
          <button className="back-btn" onClick={() => window.history.back()}>
            <ArrowLeftIcon style={{ width: 20, height: 20 }} />
          </button>
          <h1>Post</h1>
        </div>
      </div>
      <TweetCard
        tweet={tweet}
        onClick={(id) => { if (id !== tweetId) navigate(`/tweet/${id}`); }}
        onNavigateProfile={(h) => navigate(`/${h}`)}
      />
      {user && (
        <ComposeBox
          user={user}
          placeholder="Post your reply"
          replyToId={tweetId}
          onTweetCreated={handleReplyCreated}
        />
      )}
      <div className="page-header" style={{ position: 'relative' }}>
        <h1 style={{ fontSize: '1rem' }}>Replies</h1>
      </div>
      {replies.length === 0 ? (
        <EmptyState title="No replies yet" message="Be the first to reply!" />
      ) : (
        replies.map(r => (
          <TweetCard
            key={r.id}
            tweet={r}
            onClick={(id) => navigate(`/tweet/${id}`)}
            onNavigateProfile={(h) => navigate(`/${h}`)}
            compact
          />
        ))
      )}
    </>
  );
}

function ProfilePage({ handle, user, navigate }: { handle: string; user: User | null; navigate: (to: string) => void }) {
  const [profile, setProfile] = useState<UserProfileType | null>(null);
  const [tweets, setTweets] = useState<Tweet[]>([]);
  const [loading, setLoading] = useState(true);
  const [following, setFollowing] = useState(false);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.getUserProfile(handle).then(r => {
        setProfile(r.data);
        setFollowing(r.data.isFollowing);
      }),
      api.getUserTweets(handle).then(r => setTweets(r.data || [])),
    ])
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [handle]);

  const handleFollow = useCallback(async () => {
    if (!profile) return;
    try {
      const res = await api.toggleFollow(profile.id);
      setFollowing(res.following);
      setProfile(p => p ? {
        ...p,
        isFollowing: res.following,
        followerCount: p.followerCount + (res.following ? 1 : -1),
      } : p);
    } catch {}
  }, [profile]);

  if (loading) return <Spinner />;
  if (!profile) return <EmptyState title="User not found" message="This account doesn't exist." />;

  const isOwnProfile = user?.id === profile.id;

  return (
    <>
      <div className="page-header">
        <div className="page-header-back">
          <button className="back-btn" onClick={() => window.history.back()}>
            <ArrowLeftIcon style={{ width: 20, height: 20 }} />
          </button>
          <div>
            <h1>{profile.displayName}</h1>
            <div className="page-header-sub">{formatNumber(profile.tweetCount)} posts</div>
          </div>
        </div>
      </div>

      <div className="profile-header">
        <div className="profile-banner" />
        <div className="profile-info">
          <div className="profile-avatar-row">
            <Avatar user={profile} size="lg" />
            {!isOwnProfile && user && (
              <button
                className={`btn-follow ${following ? 'following' : 'not-following'}`}
                onClick={handleFollow}
              >
                {following ? 'Following' : 'Follow'}
              </button>
            )}
          </div>
          <div className="profile-name">{profile.displayName}</div>
          <div className="profile-handle">@{profile.handle}</div>
          {profile.bio && <div className="profile-bio">{profile.bio}</div>}
          <div className="profile-meta">
            {profile.location && (
              <span><MapPinIcon /> {profile.location}</span>
            )}
            {profile.website && (
              <span><LinkIcon /> {profile.website}</span>
            )}
            <span><CalendarIcon /> Joined {formatDate(profile.createdAt)}</span>
          </div>
          <div className="profile-stats">
            <span><strong>{formatNumber(profile.followingCount)}</strong> Following</span>
            <span><strong>{formatNumber(profile.followerCount)}</strong> Followers</span>
          </div>
        </div>
      </div>

      <div className="tabs">
        <div className="tab active">Posts</div>
        <div className="tab">Replies</div>
        <div className="tab">Likes</div>
      </div>

      {tweets.length === 0 ? (
        <EmptyState title="No posts yet" message={`@${profile.handle} hasn't posted anything.`} />
      ) : (
        tweets.map(t => (
          <TweetCard
            key={t.id}
            tweet={t}
            onClick={(id) => navigate(`/tweet/${id}`)}
            onNavigateProfile={(h) => navigate(`/${h}`)}
          />
        ))
      )}
    </>
  );
}

function SearchPage({ navigate, user }: { navigate: (to: string) => void; user: User | null }) {
  const [query, setQuery] = useState('');
  const [tab, setTab] = useState<'tweets' | 'users'>('tweets');
  const [tweetResults, setTweetResults] = useState<Tweet[]>([]);
  const [userResults, setUserResults] = useState<User[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);

  const doSearch = useCallback(async () => {
    if (!query.trim()) return;
    setLoading(true);
    setSearched(true);
    try {
      const res = await api.search(query.trim());
      setTweetResults(res.tweets || []);
      setUserResults(res.users || []);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [query]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter') doSearch();
  }, [doSearch]);

  return (
    <>
      <div className="page-header">
        <div className="search-bar">
          <SearchIcon />
          <input
            type="text"
            placeholder="Search Chirp"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            autoFocus
          />
        </div>
      </div>

      {searched && (
        <div className="tabs">
          <button className={`tab ${tab === 'tweets' ? 'active' : ''}`} onClick={() => setTab('tweets')}>
            Tweets
          </button>
          <button className={`tab ${tab === 'users' ? 'active' : ''}`} onClick={() => setTab('users')}>
            People
          </button>
        </div>
      )}

      {loading ? (
        <Spinner />
      ) : !searched ? (
        <EmptyState title="Search Chirp" message="Find tweets, people, and more." />
      ) : tab === 'tweets' ? (
        tweetResults.length === 0 ? (
          <EmptyState title="No results" message={`No tweets found for "${query}"`} />
        ) : (
          tweetResults.map(t => (
            <TweetCard
              key={t.id}
              tweet={t}
              onClick={(id) => navigate(`/tweet/${id}`)}
              onNavigateProfile={(h) => navigate(`/${h}`)}
            />
          ))
        )
      ) : (
        userResults.length === 0 ? (
          <EmptyState title="No results" message={`No people found for "${query}"`} />
        ) : (
          userResults.map(u => (
            <div
              key={u.id}
              className="who-to-follow-item"
              onClick={() => navigate(`/${u.handle}`)}
              style={{ cursor: 'pointer', borderBottom: '1px solid var(--border)' }}
            >
              <Avatar user={u} />
              <div className="who-to-follow-info">
                <div className="who-to-follow-name">{u.displayName}</div>
                <div className="who-to-follow-handle">@{u.handle}</div>
                {u.bio && <div style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: 2 }}>{u.bio}</div>}
              </div>
            </div>
          ))
        )
      )}
    </>
  );
}

function NotificationsPage({ user, navigate }: { user: User | null; navigate: (to: string) => void }) {
  const [notifications, setNotifications] = useState<NotifType[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!user) {
      setLoading(false);
      return;
    }
    api.getNotifications()
      .then(res => setNotifications(res.data || []))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [user]);

  if (!user) {
    return <EmptyState title="Sign in" message="Log in to see your notifications." />;
  }

  const notifIcon = (type: string) => {
    switch (type) {
      case 'like': return <div className="notification-icon like"><HeartIcon /></div>;
      case 'retweet': return <div className="notification-icon retweet"><RepeatIcon /></div>;
      case 'follow': return <div className="notification-icon follow"><UserIcon /></div>;
      case 'reply': return <div className="notification-icon reply"><MessageIcon /></div>;
      case 'quote': return <div className="notification-icon quote"><QuoteIcon /></div>;
      default: return <div className="notification-icon"><BellIcon /></div>;
    }
  };

  const notifText = (n: NotifType) => {
    const name = n.actor?.displayName || 'Someone';
    switch (n.type) {
      case 'like': return <><strong>{name}</strong> liked your tweet</>;
      case 'retweet': return <><strong>{name}</strong> retweeted your tweet</>;
      case 'follow': return <><strong>{name}</strong> followed you</>;
      case 'reply': return <><strong>{name}</strong> replied to your tweet</>;
      case 'quote': return <><strong>{name}</strong> quoted your tweet</>;
      default: return <><strong>{name}</strong> interacted with you</>;
    }
  };

  return (
    <>
      <div className="page-header">
        <h1>Notifications</h1>
      </div>
      {loading ? (
        <Spinner />
      ) : notifications.length === 0 ? (
        <EmptyState title="Nothing here yet" message="When someone interacts with you, you'll see it here." />
      ) : (
        notifications.map(n => (
          <div
            key={n.id}
            className={`notification ${!n.read ? 'unread' : ''}`}
            onClick={() => {
              if (n.tweetId) navigate(`/tweet/${n.tweetId}`);
              else if (n.actor?.handle) navigate(`/${n.actor.handle}`);
            }}
            style={{ cursor: 'pointer' }}
          >
            {notifIcon(n.type)}
            <div className="notification-body">
              <div style={{ marginBottom: 4 }}>
                {n.actor && <Avatar user={n.actor} size="sm" />}
              </div>
              <div className="notification-text">{notifText(n)}</div>
              <div style={{ color: 'var(--text-muted)', fontSize: '0.8rem', marginTop: 2 }}>
                {timeAgo(n.createdAt)}
              </div>
            </div>
          </div>
        ))
      )}
    </>
  );
}

function AuthPage({ mode, navigate, onAuth }: { mode: 'login' | 'register'; navigate: (to: string) => void; onAuth: (email: string, password: string, handle?: string, displayName?: string) => Promise<void> }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [handle, setHandle] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setSubmitting(true);
    try {
      await onAuth(email, password, handle, displayName);
      navigate('/');
    } catch (err: any) {
      setError(err.message || 'Something went wrong');
    } finally {
      setSubmitting(false);
    }
  }, [email, password, handle, displayName, onAuth, navigate]);

  return (
    <div className="auth-page">
      <div className="auth-card">
        <div style={{ textAlign: 'center', marginBottom: 20 }}>
          <LogoIcon style={{ width: 40, height: 40 }} />
        </div>
        <h1>{mode === 'login' ? 'Sign in' : 'Create account'}</h1>
        <p>{mode === 'login' ? 'Welcome back to Chirp.' : 'Join the conversation.'}</p>
        <form onSubmit={handleSubmit}>
          {mode === 'register' && (
            <>
              <div className="form-group">
                <label>Display Name</label>
                <input className="form-input" type="text" value={displayName} onChange={e => setDisplayName(e.target.value)} required />
              </div>
              <div className="form-group">
                <label>Handle</label>
                <input className="form-input" type="text" value={handle} onChange={e => setHandle(e.target.value)} required placeholder="username" />
              </div>
            </>
          )}
          <div className="form-group">
            <label>Email</label>
            <input className="form-input" type="email" value={email} onChange={e => setEmail(e.target.value)} required />
          </div>
          <div className="form-group">
            <label>Password</label>
            <input className="form-input" type="password" value={password} onChange={e => setPassword(e.target.value)} required />
          </div>
          {error && <div className="form-error">{error}</div>}
          <button className="btn-primary" type="submit" disabled={submitting} style={{ width: '100%', marginTop: 16, padding: '12px' }}>
            {submitting ? 'Loading...' : mode === 'login' ? 'Sign in' : 'Create account'}
          </button>
        </form>
        <div className="auth-footer">
          {mode === 'login' ? (
            <>Don't have an account? <a href="/register" onClick={(e) => { e.preventDefault(); navigate('/register'); }}>Sign up</a></>
          ) : (
            <>Already have an account? <a href="/login" onClick={(e) => { e.preventDefault(); navigate('/login'); }}>Sign in</a></>
          )}
        </div>
      </div>
    </div>
  );
}

// ---- Right Sidebar ----
function RightSidebar({ navigate }: { navigate: (to: string) => void }) {
  const [stats, setStats] = useState<Record<string, number>>({});

  useEffect(() => {
    api.getStats().then(r => setStats(r.data || {})).catch(() => {});
  }, []);

  return (
    <div className="right-sidebar">
      <div className="search-bar" onClick={() => navigate('/search')} style={{ cursor: 'text' }}>
        <SearchIcon />
        <input type="text" placeholder="Search Chirp" readOnly style={{ cursor: 'text' }} />
      </div>

      <div className="trending-card">
        <h3>Platform Stats</h3>
        {Object.entries(stats).map(([key, val]) => (
          <div key={key} className="trending-item">
            <div className="trending-label">Total</div>
            <div className="trending-topic" style={{ textTransform: 'capitalize' }}>{key}</div>
            <div className="trending-count">{formatNumber(val)}</div>
          </div>
        ))}
      </div>

      <div className="trending-card">
        <h3>Powered by Flop</h3>
        <div style={{ padding: '0 16px', color: 'var(--text-secondary)', fontSize: '0.9rem', lineHeight: 1.5 }}>
          This Twitter clone is built entirely on the Flop embedded database engine with Go backend and React frontend.
        </div>
      </div>
    </div>
  );
}

// ---- Compose Modal ----
function ComposeModal({ user, onClose, onCreated, navigate }: {
  user: User;
  onClose: () => void;
  onCreated: (tweet: Tweet) => void;
  navigate: (to: string) => void;
}) {
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <button className="modal-close" onClick={onClose}>
            <XIcon style={{ width: 20, height: 20 }} />
          </button>
        </div>
        <ComposeBox user={user} onTweetCreated={(t) => { onCreated(t); onClose(); }} />
      </div>
    </div>
  );
}

// ---- Main App ----
function App() {
  const { path, navigate } = useRouter();
  const { user, loading: authLoading, loginUser, registerUser, logout } = useAuth();
  const [showCompose, setShowCompose] = useState(false);

  // Route matching
  const route = useMemo(() => {
    if (path === '/' || path === '/home') return { page: 'home' as const };
    if (path === '/search' || path === '/explore') return { page: 'search' as const };
    if (path === '/notifications') return { page: 'notifications' as const };
    if (path === '/login') return { page: 'login' as const };
    if (path === '/register') return { page: 'register' as const };
    if (path.startsWith('/tweet/')) return { page: 'tweet' as const, id: path.slice(7) };
    // Anything else = profile by handle
    const handle = path.slice(1);
    if (handle && !handle.includes('/')) return { page: 'profile' as const, handle };
    return { page: 'home' as const };
  }, [path]);

  if (authLoading) {
    return <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh' }}><Spinner /></div>;
  }

  // Auth pages render without layout
  if (route.page === 'login') {
    return (
      <AuthPage
        mode="login"
        navigate={navigate}
        onAuth={async (email, password) => { await loginUser(email, password); }}
      />
    );
  }
  if (route.page === 'register') {
    return (
      <AuthPage
        mode="register"
        navigate={navigate}
        onAuth={async (email, password, handle, displayName) => {
          await registerUser({ email, password, handle: handle!, displayName: displayName! });
        }}
      />
    );
  }

  const navItems = [
    { path: '/', icon: HomeIcon, label: 'Home' },
    { path: '/search', icon: SearchIcon, label: 'Explore' },
    { path: '/notifications', icon: BellIcon, label: 'Notifications' },
    ...(user ? [{ path: `/${user.handle}`, icon: UserIcon, label: 'Profile' }] : []),
  ];

  return (
    <div className="app-layout">
      {/* Sidebar */}
      <nav className="sidebar">
        <div className="sidebar-logo" onClick={() => navigate('/')} style={{ cursor: 'pointer' }}>
          <LogoIcon />
        </div>
        <div className="sidebar-nav">
          {navItems.map(item => (
            <a
              key={item.path}
              href={item.path}
              className={`nav-item ${path === item.path ? 'active' : ''}`}
              onClick={(e) => { e.preventDefault(); navigate(item.path); }}
            >
              <item.icon />
              <span>{item.label}</span>
            </a>
          ))}
        </div>
        {user ? (
          <>
            <button className="nav-compose-btn" onClick={() => setShowCompose(true)}>
              <span>Post</span>
              <FeatherIcon style={{ width: 22, height: 22, display: 'none' }} />
            </button>
            <div className="sidebar-profile" onClick={() => navigate(`/${user.handle}`)}>
              <Avatar user={user} size="sm" />
              <div className="profile-mini-info" style={{ minWidth: 0 }}>
                <div style={{ fontWeight: 700, fontSize: '0.9rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {user.displayName}
                </div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '0.82rem' }}>@{user.handle}</div>
              </div>
            </div>
          </>
        ) : (
          <a
            href="/login"
            className="nav-compose-btn"
            onClick={(e) => { e.preventDefault(); navigate('/login'); }}
            style={{ textAlign: 'center', display: 'block' }}
          >
            <span>Sign in</span>
          </a>
        )}
      </nav>

      {/* Main Content */}
      <main className="main-content">
        {route.page === 'home' && <HomePage user={user} navigate={navigate} />}
        {route.page === 'tweet' && <TweetDetailPage tweetId={route.id!} user={user} navigate={navigate} />}
        {route.page === 'profile' && <ProfilePage handle={route.handle!} user={user} navigate={navigate} />}
        {route.page === 'search' && <SearchPage navigate={navigate} user={user} />}
        {route.page === 'notifications' && <NotificationsPage user={user} navigate={navigate} />}
      </main>

      {/* Right Sidebar */}
      <RightSidebar navigate={navigate} />

      {/* Compose Modal */}
      {showCompose && user && (
        <ComposeModal
          user={user}
          onClose={() => setShowCompose(false)}
          onCreated={() => {}}
          navigate={navigate}
        />
      )}
    </div>
  );
}

// Mount
const container = document.getElementById('app');
if (container) {
  createRoot(container).render(<App />);
}

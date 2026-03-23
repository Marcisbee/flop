import React, { useState, useCallback } from 'react';
import type { Tweet, User } from './api';
import { toggleLike, toggleRetweet, createTweet } from './api';
import { HeartIcon, HeartFilledIcon, RepeatIcon, MessageIcon, QuoteIcon } from './icons';
import { timeAgo, formatNumber } from './utils';

// ---- Avatar ----
export function Avatar({ user, size = 'md' }: { user?: User | null; size?: 'sm' | 'md' | 'lg' }) {
  const cls = `avatar ${size === 'sm' ? 'avatar-sm' : size === 'lg' ? 'avatar-lg' : ''}`;
  if (user?.avatarUrl) {
    return <img src={user.avatarUrl} alt="" className={cls} />;
  }
  const initials = user ? (user.displayName || user.handle || '?').charAt(0).toUpperCase() : '?';
  return <div className={`${cls} avatar-placeholder`}>{initials}</div>;
}

// ---- Tweet Card ----
export function TweetCard({
  tweet,
  onClick,
  onNavigateProfile,
  compact = false,
}: {
  tweet: Tweet;
  onClick?: (id: string) => void;
  onNavigateProfile?: (handle: string) => void;
  compact?: boolean;
}) {
  const [liked, setLiked] = useState(tweet.liked);
  const [likeCount, setLikeCount] = useState(tweet.likeCount);
  const [retweeted, setRetweeted] = useState(tweet.retweeted);
  const [retweetCount, setRetweetCount] = useState(tweet.retweetCount);

  const handleLike = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      const res = await toggleLike(tweet.id);
      setLiked(res.liked);
      setLikeCount(c => c + (res.liked ? 1 : -1));
    } catch {}
  }, [tweet.id]);

  const handleRetweet = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      const res = await toggleRetweet(tweet.id);
      setRetweeted(res.retweeted);
      setRetweetCount(c => c + (res.retweeted ? 1 : -1));
    } catch {}
  }, [tweet.id]);

  const handleProfileClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    if (tweet.author?.handle && onNavigateProfile) {
      onNavigateProfile(tweet.author.handle);
    }
  }, [tweet.author, onNavigateProfile]);

  return (
    <div className="tweet" onClick={() => onClick?.(tweet.id)}>
      <div onClick={handleProfileClick} style={{ cursor: 'pointer' }}>
        <Avatar user={tweet.author} />
      </div>
      <div className="tweet-body">
        {tweet.replyToId && !compact && (
          <div className="tweet-reply-context">
            Replying to a thread
          </div>
        )}
        <div className="tweet-header">
          <span className="tweet-author" onClick={handleProfileClick} style={{ cursor: 'pointer' }}>
            {tweet.author?.displayName || 'Unknown'}
          </span>
          <span className="tweet-handle" onClick={handleProfileClick} style={{ cursor: 'pointer' }}>
            @{tweet.author?.handle || '???'}
          </span>
          <span className="tweet-time">&middot; {timeAgo(tweet.createdAt)}</span>
        </div>
        <div className="tweet-content">{tweet.content}</div>

        {tweet.quotedTweet && (
          <div className="tweet-quote" onClick={(e) => { e.stopPropagation(); onClick?.(tweet.quotedTweet!.id); }}>
            <div className="tweet-header">
              <span className="tweet-author">{tweet.quotedTweet.author?.displayName}</span>
              <span className="tweet-handle">@{tweet.quotedTweet.author?.handle}</span>
            </div>
            <div className="tweet-content">{tweet.quotedTweet.content}</div>
          </div>
        )}

        <div className="tweet-actions">
          <button className="tweet-action reply" onClick={(e) => { e.stopPropagation(); onClick?.(tweet.id); }}>
            <MessageIcon />
            {tweet.replyCount > 0 && <span>{formatNumber(tweet.replyCount)}</span>}
          </button>
          <button className={`tweet-action ${retweeted ? 'retweeted' : ''}`} onClick={handleRetweet}>
            <RepeatIcon />
            {retweetCount > 0 && <span>{formatNumber(retweetCount)}</span>}
          </button>
          <button className={`tweet-action ${liked ? 'liked' : ''}`} onClick={handleLike}>
            {liked ? <HeartFilledIcon /> : <HeartIcon />}
            {likeCount > 0 && <span>{formatNumber(likeCount)}</span>}
          </button>
          <button className="tweet-action" onClick={(e) => { e.stopPropagation(); }}>
            <QuoteIcon />
            {tweet.quoteCount > 0 && <span>{formatNumber(tweet.quoteCount)}</span>}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---- Compose Box ----
export function ComposeBox({
  placeholder = "What's happening?",
  replyToId,
  quoteOfId,
  user,
  onTweetCreated,
}: {
  placeholder?: string;
  replyToId?: string;
  quoteOfId?: string;
  user?: User | null;
  onTweetCreated?: (tweet: Tweet) => void;
}) {
  const [content, setContent] = useState('');
  const [posting, setPosting] = useState(false);

  const remaining = 280 - content.length;

  const handlePost = useCallback(async () => {
    if (!content.trim() || content.length > 280 || posting) return;
    setPosting(true);
    try {
      const res = await createTweet({ content: content.trim(), replyToId, quoteOfId });
      setContent('');
      onTweetCreated?.(res.data);
    } catch (err) {
      console.error('Failed to post:', err);
    } finally {
      setPosting(false);
    }
  }, [content, replyToId, quoteOfId, posting, onTweetCreated]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      handlePost();
    }
  }, [handlePost]);

  return (
    <div className="compose-area">
      <Avatar user={user} />
      <div className="compose-input-wrap">
        <textarea
          className="compose-textarea"
          placeholder={placeholder}
          value={content}
          onChange={(e) => setContent(e.target.value)}
          onKeyDown={handleKeyDown}
          rows={2}
        />
        <div className="compose-actions">
          <span className={`compose-counter ${remaining < 20 ? (remaining < 0 ? 'over' : 'warn') : ''}`}>
            {remaining}
          </span>
          <button
            className="btn-primary"
            disabled={!content.trim() || content.length > 280 || posting}
            onClick={handlePost}
          >
            {replyToId ? 'Reply' : 'Post'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---- Loading Spinner ----
export function Spinner() {
  return <div className="loading"><div className="spinner" /></div>;
}

// ---- Empty State ----
export function EmptyState({ title, message }: { title: string; message: string }) {
  return (
    <div className="empty-state">
      <h3>{title}</h3>
      <p>{message}</p>
    </div>
  );
}

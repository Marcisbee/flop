import React, { useState, useCallback } from 'react';
import type { Post, Comment, User, Community } from './api';
import * as api from './api';
import { ArrowUpIcon, ArrowDownIcon, MessageIcon, ShareIcon } from './icons';
import { timeAgo, formatNumber } from './utils';

// ---- Vote Buttons ----
export function VoteButtons({
  score,
  onVote,
  vertical = true,
}: {
  score: number;
  onVote: (value: number) => void;
  vertical?: boolean;
}) {
  return (
    <div className={`vote-buttons ${vertical ? 'vertical' : 'horizontal'}`}>
      <button className="vote-btn upvote" onClick={() => onVote(1)}>
        <ArrowUpIcon />
      </button>
      <span className="vote-score">{formatNumber(score)}</span>
      <button className="vote-btn downvote" onClick={() => onVote(-1)}>
        <ArrowDownIcon />
      </button>
    </div>
  );
}

// ---- Post Card ----
export function PostCard({
  post,
  onClick,
  onCommunityClick,
  onUserClick,
  showCommunity = true,
}: {
  post: Post;
  onClick?: (id: number) => void;
  onCommunityClick?: (handle: string) => void;
  onUserClick?: (id: number) => void;
  showCommunity?: boolean;
}) {
  const [score, setScore] = useState(post.score);

  const handleVote = useCallback(async (value: number) => {
    try {
      const res = await api.votePost(post.id, value);
      setScore(res.score);
    } catch {}
  }, [post.id]);

  return (
    <div className="post-card" onClick={() => onClick?.(post.id)}>
      <VoteButtons score={score} onVote={handleVote} />
      <div className="post-content">
        <div className="post-meta">
          {showCommunity && post._community_id && (
            <span
              className="post-community"
              onClick={(e) => { e.stopPropagation(); onCommunityClick?.(post._community_id!.handle); }}
            >
              r/{post._community_id.handle}
            </span>
          )}
          {post._author_id && (
            <span
              className="post-author"
              onClick={(e) => { e.stopPropagation(); onUserClick?.(post.author_id); }}
            >
              Posted by u/{post._author_id.handle}
            </span>
          )}
          <span className="post-time">{timeAgo(post.created_at)}</span>
        </div>
        <h3 className="post-title">{post.title}</h3>
        {post.body && <p className="post-body">{post.body.length > 300 ? post.body.slice(0, 300) + '...' : post.body}</p>}
        {post.link && (
          <a className="post-link" href={post.link} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()}>
            {new URL(post.link).hostname}
          </a>
        )}
        {post.image && <img className="post-image" src={post.image} alt="" />}
        <div className="post-actions">
          <button className="post-action" onClick={(e) => { e.stopPropagation(); onClick?.(post.id); }}>
            <MessageIcon />
            <span>{formatNumber(post.comment_count)} Comments</span>
          </button>
          <button className="post-action">
            <ShareIcon />
            <span>Share</span>
          </button>
        </div>
      </div>
    </div>
  );
}

// ---- Comment Tree ----
export function CommentItem({
  comment,
  onReply,
  user,
}: {
  comment: Comment;
  onReply: (parentId: number) => void;
  user?: User | null;
}) {
  const [score, setScore] = useState(comment.score);

  const handleVote = useCallback(async (value: number) => {
    try {
      const res = await api.voteComment(comment.id, value);
      setScore(res.score);
    } catch {}
  }, [comment.id]);

  return (
    <div className="comment" style={{ marginLeft: Math.min(comment.depth * 24, 120) }}>
      <div className="comment-header">
        <span className="comment-author">u/{comment._author_id?.handle || '???'}</span>
        <span className="comment-time">{timeAgo(comment.created_at)}</span>
      </div>
      <div className="comment-body">{comment.body}</div>
      <div className="comment-actions">
        <VoteButtons score={score} onVote={handleVote} vertical={false} />
        {user && (
          <button className="comment-reply-btn" onClick={() => onReply(comment.id)}>
            Reply
          </button>
        )}
      </div>
    </div>
  );
}

// ---- Create Post Form ----
export function CreatePostForm({
  communities,
  onCreated,
}: {
  communities: Community[];
  onCreated?: () => void;
}) {
  const [title, setTitle] = useState('');
  const [body, setBody] = useState('');
  const [link, setLink] = useState('');
  const [communityId, setCommunityId] = useState<number>(communities[0]?.id || 0);
  const [tab, setTab] = useState<'text' | 'link'>('text');
  const [posting, setPosting] = useState(false);

  const handleSubmit = useCallback(async () => {
    if (!title.trim() || !communityId || posting) return;
    setPosting(true);
    try {
      await api.createPost(
        title.trim(),
        communityId,
        tab === 'text' ? body.trim() : undefined,
        tab === 'link' ? link.trim() : undefined,
      );
      setTitle('');
      setBody('');
      setLink('');
      onCreated?.();
    } catch (err) {
      console.error('Failed to create post:', err);
    } finally {
      setPosting(false);
    }
  }, [title, body, link, communityId, tab, posting, onCreated]);

  return (
    <div className="create-post-form">
      <h3>Create a post</h3>
      <select value={communityId} onChange={e => setCommunityId(Number(e.target.value))}>
        {communities.map(c => (
          <option key={c.id} value={c.id}>r/{c.handle}</option>
        ))}
      </select>
      <div className="post-type-tabs">
        <button className={tab === 'text' ? 'active' : ''} onClick={() => setTab('text')}>Text</button>
        <button className={tab === 'link' ? 'active' : ''} onClick={() => setTab('link')}>Link</button>
      </div>
      <input
        type="text"
        placeholder="Title"
        value={title}
        onChange={e => setTitle(e.target.value)}
        maxLength={300}
      />
      {tab === 'text' && (
        <textarea
          placeholder="Text (optional)"
          value={body}
          onChange={e => setBody(e.target.value)}
          rows={4}
        />
      )}
      {tab === 'link' && (
        <input
          type="url"
          placeholder="URL"
          value={link}
          onChange={e => setLink(e.target.value)}
        />
      )}
      <button className="btn-primary" onClick={handleSubmit} disabled={!title.trim() || !communityId || posting}>
        {posting ? 'Posting...' : 'Post'}
      </button>
    </div>
  );
}

// ---- Community Card ----
export function CommunityCard({
  community,
  onClick,
}: {
  community: Community;
  onClick?: (handle: string) => void;
}) {
  return (
    <div className="community-card" onClick={() => onClick?.(community.handle)}>
      <div className="community-icon">r/</div>
      <div>
        <div className="community-name">r/{community.handle}</div>
        <div className="community-members">{formatNumber(community.member_count)} members</div>
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

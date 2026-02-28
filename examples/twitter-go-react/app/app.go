package app

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/marcisbee/flop"
)

type HeadMeta struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type HeadPayload struct {
	Title string     `json:"title"`
	Meta  []HeadMeta `json:"meta,omitempty"`
}

// Build creates the flop App with all table definitions.
func Build() *flop.App {
	return BuildWithDataDir("./data")
}

// BuildWithDataDir creates the flop App with a custom data dir.
func BuildWithDataDir(dataDir string) *flop.App {
	application := flop.New(flop.Config{
		DataDir:               dataDir,
		SyncMode:              "normal",
		AsyncSecondaryIndexes: true,
		RequestLogRetention:   7 * 24 * time.Hour,
	})

	users := flop.Define(application, "users", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Autogen(`[a-z0-9]{12}`)
		s.String("email").Required().Unique().Email().MaxLen(255)
		s.Bcrypt("password", 10).Required()
		s.String("handle").Required().Unique().MinLen(1).MaxLen(30)
		s.String("displayName").Required().MinLen(1).MaxLen(80)
		s.String("bio").MaxLen(280)
		s.String("avatarUrl")
		s.String("headerUrl")
		s.String("location").MaxLen(100)
		s.String("website").MaxLen(200)
		s.Roles("roles")
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.Define(application, "tweets", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Autogen(`[a-z0-9]{16}`)
		s.Ref("authorId", users, "id").Required().Index()
		s.String("content").Required().MaxLen(280).FullText()
		s.String("replyToId").Index() // tweet id this is replying to
		s.String("quoteOfId").Index() // tweet id this is quoting (quote retweet)
		s.Cached("replyCount", flop.Int).
			OnChange("tweets", "replyToId").
			Compute(func(row flop.Row, db *flop.Database) any {
				return db.Table("tweets").CountByIndex("replyToId", row.ID())
			})
		s.Cached("retweetCount", flop.Int).
			OnChange("retweets", "tweetId").
			Compute(func(row flop.Row, db *flop.Database) any {
				return db.Table("retweets").CountByIndex("tweetId", row.ID())
			})
		s.Cached("likeCount", flop.Int).
			OnChange("likes", "tweetId").
			Compute(func(row flop.Row, db *flop.Database) any {
				return db.Table("likes").CountByIndex("tweetId", row.ID())
			})
		s.Cached("quoteCount", flop.Int).
			OnChange("tweets", "quoteOfId").
			Compute(func(row flop.Row, db *flop.Database) any {
				return db.Table("tweets").CountByIndex("quoteOfId", row.ID())
			})
		s.Timestamp("createdAt").DefaultNow()
	})

	// Likes: composite key = "userId:tweetId"
	flop.Define(application, "likes", func(s *flop.SchemaBuilder) {
		s.String("id").Primary() // format: userId:tweetId
		s.Ref("userId", users, "id").Required()
		s.String("tweetId").Required().Index()
		s.Timestamp("createdAt").DefaultNow()
	})

	// Retweets: composite key = "userId:tweetId"
	flop.Define(application, "retweets", func(s *flop.SchemaBuilder) {
		s.String("id").Primary() // format: userId:tweetId
		s.Ref("userId", users, "id").Required()
		s.String("tweetId").Required().Index()
		s.Timestamp("createdAt").DefaultNow()
	})

	// Follows: composite key = "followerId:followingId"
	flop.Define(application, "follows", func(s *flop.SchemaBuilder) {
		s.String("id").Primary() // format: followerId:followingId
		s.Ref("followerId", users, "id").Required().Index()
		s.Ref("followingId", users, "id").Required().Index()
		s.Timestamp("createdAt").DefaultNow()
	})

	// Notifications
	flop.Define(application, "notifications", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Autogen(`[a-z0-9]{16}`)
		s.Ref("userId", users, "id").Required().Index() // who receives
		s.Ref("actorId", users, "id").Required()        // who caused
		s.Enum("type", "like", "retweet", "reply", "follow", "quote").Required()
		s.String("tweetId") // related tweet (if applicable)
		s.Boolean("read").Default(false)
		s.Timestamp("createdAt").DefaultNow()
	})

	return application
}

// ---- Query helpers ----

// TweetWithAuthor enriches a tweet row with author data.
type TweetWithAuthor struct {
	ID           string           `json:"id"`
	AuthorID     string           `json:"authorId"`
	Content      string           `json:"content"`
	ReplyToID    string           `json:"replyToId,omitempty"`
	QuoteOfID    string           `json:"quoteOfId,omitempty"`
	ReplyCount   int              `json:"replyCount"`
	RetweetCount int              `json:"retweetCount"`
	LikeCount    int              `json:"likeCount"`
	QuoteCount   int              `json:"quoteCount"`
	CreatedAt    float64          `json:"createdAt"`
	Author       *UserPublic      `json:"author,omitempty"`
	QuotedTweet  *TweetWithAuthor `json:"quotedTweet,omitempty"`
	Liked        bool             `json:"liked"`
	Retweeted    bool             `json:"retweeted"`
}

type UserPublic struct {
	ID          string  `json:"id"`
	Handle      string  `json:"handle"`
	DisplayName string  `json:"displayName"`
	Bio         string  `json:"bio,omitempty"`
	AvatarUrl   string  `json:"avatarUrl,omitempty"`
	HeaderUrl   string  `json:"headerUrl,omitempty"`
	Location    string  `json:"location,omitempty"`
	Website     string  `json:"website,omitempty"`
	CreatedAt   float64 `json:"createdAt"`
}

type UserProfile struct {
	UserPublic
	FollowerCount  int  `json:"followerCount"`
	FollowingCount int  `json:"followingCount"`
	TweetCount     int  `json:"tweetCount"`
	IsFollowing    bool `json:"isFollowing"`
}

func ToUserPublic(row map[string]any) *UserPublic {
	if row == nil {
		return nil
	}
	return &UserPublic{
		ID:          Str(row["id"]),
		Handle:      Str(row["handle"]),
		DisplayName: Str(row["displayName"]),
		Bio:         Str(row["bio"]),
		AvatarUrl:   Str(row["avatarUrl"]),
		HeaderUrl:   Str(row["headerUrl"]),
		Location:    Str(row["location"]),
		Website:     Str(row["website"]),
		CreatedAt:   Num(row["createdAt"]),
	}
}

func ToTweetWithAuthor(row map[string]any, db *flop.Database, viewerID string) *TweetWithAuthor {
	if row == nil {
		return nil
	}
	t := &TweetWithAuthor{
		ID:           Str(row["id"]),
		AuthorID:     Str(row["authorId"]),
		Content:      Str(row["content"]),
		ReplyToID:    Str(row["replyToId"]),
		QuoteOfID:    Str(row["quoteOfId"]),
		ReplyCount:   Integer(row["replyCount"]),
		RetweetCount: Integer(row["retweetCount"]),
		LikeCount:    Integer(row["likeCount"]),
		QuoteCount:   Integer(row["quoteCount"]),
		CreatedAt:    Num(row["createdAt"]),
	}

	// Attach author
	users := db.Table("users")
	if users != nil {
		authorRow, err := users.Get(t.AuthorID)
		if err == nil && authorRow != nil {
			t.Author = ToUserPublic(authorRow)
		}
	}

	// Attach quoted tweet
	if t.QuoteOfID != "" {
		tweets := db.Table("tweets")
		if tweets != nil {
			qtRow, err := tweets.Get(t.QuoteOfID)
			if err == nil && qtRow != nil {
				t.QuotedTweet = ToTweetWithAuthor(qtRow, db, "")
			}
		}
	}

	// Check liked/retweeted status for viewer
	if viewerID != "" {
		likes := db.Table("likes")
		if likes != nil {
			likeID := viewerID + ":" + t.ID
			row, err := likes.Get(likeID)
			t.Liked = err == nil && row != nil
		}
		retweets := db.Table("retweets")
		if retweets != nil {
			rtID := viewerID + ":" + t.ID
			row, err := retweets.Get(rtID)
			t.Retweeted = err == nil && row != nil
		}
	}

	return t
}

// ListTimeline returns tweets for the home feed (all tweets, newest first).
func ListTimeline(db *flop.Database, limit, offset int, viewerID string) []*TweetWithAuthor {
	tweets := db.Table("tweets")
	if tweets == nil {
		return nil
	}

	rows, err := tweets.Scan(limit+offset+500, 0)
	if err != nil {
		return nil
	}

	// Filter out replies for timeline
	var filtered []map[string]any
	for _, row := range rows {
		if Str(row["replyToId"]) == "" {
			filtered = append(filtered, row)
		}
	}

	// Sort by createdAt descending
	sort.Slice(filtered, func(i, j int) bool {
		return Num(filtered[i]["createdAt"]) > Num(filtered[j]["createdAt"])
	})

	// Paginate
	if offset >= len(filtered) {
		return nil
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	filtered = filtered[offset:end]

	result := make([]*TweetWithAuthor, 0, len(filtered))
	for _, row := range filtered {
		result = append(result, ToTweetWithAuthor(row, db, viewerID))
	}
	return result
}

// ListUserTweets returns tweets by a specific user.
func ListUserTweets(db *flop.Database, userID string, limit, offset int, viewerID string) []*TweetWithAuthor {
	tweets := db.Table("tweets")
	if tweets == nil {
		return nil
	}

	userTweets, err := tweets.FindByIndex("authorId", userID)
	if err != nil {
		return nil
	}

	sort.Slice(userTweets, func(i, j int) bool {
		return Num(userTweets[i]["createdAt"]) > Num(userTweets[j]["createdAt"])
	})

	if offset >= len(userTweets) {
		return nil
	}
	end := offset + limit
	if end > len(userTweets) {
		end = len(userTweets)
	}
	userTweets = userTweets[offset:end]

	result := make([]*TweetWithAuthor, 0, len(userTweets))
	for _, row := range userTweets {
		result = append(result, ToTweetWithAuthor(row, db, viewerID))
	}
	return result
}

// ListReplies returns replies to a specific tweet.
func ListReplies(db *flop.Database, tweetID string, limit, offset int, viewerID string) []*TweetWithAuthor {
	tweets := db.Table("tweets")
	if tweets == nil {
		return nil
	}

	replies, err := tweets.FindByIndex("replyToId", tweetID)
	if err != nil {
		return nil
	}

	sort.Slice(replies, func(i, j int) bool {
		return Num(replies[i]["createdAt"]) > Num(replies[j]["createdAt"])
	})

	if offset >= len(replies) {
		return nil
	}
	end := offset + limit
	if end > len(replies) {
		end = len(replies)
	}
	replies = replies[offset:end]

	result := make([]*TweetWithAuthor, 0, len(replies))
	for _, row := range replies {
		result = append(result, ToTweetWithAuthor(row, db, viewerID))
	}
	return result
}

// SearchTweets performs full-text search on tweets.
func SearchTweets(db *flop.Database, query string, limit int, viewerID string) []*TweetWithAuthor {
	tweets := db.Table("tweets")
	if tweets == nil {
		return nil
	}

	rows, err := tweets.SearchFullText([]string{"content"}, query, limit)
	if err != nil {
		return nil
	}

	result := make([]*TweetWithAuthor, 0, len(rows))
	for _, row := range rows {
		result = append(result, ToTweetWithAuthor(row, db, viewerID))
	}
	return result
}

// SearchUsers uses autocomplete to find users by handle/name.
func SearchUsers(db *flop.Database, autocomplete *flop.AutocompleteIndex, query string, limit int) []*UserPublic {
	entries := autocomplete.Query(query, limit)
	users := db.Table("users")
	if users == nil {
		return nil
	}

	result := make([]*UserPublic, 0, len(entries))
	for _, entry := range entries {
		row, err := users.Get(entry.Key)
		if err == nil && row != nil {
			result = append(result, ToUserPublic(row))
		}
	}
	return result
}

// GetUserProfile builds a full profile with counts.
func GetUserProfile(db *flop.Database, userID string, viewerID string) *UserProfile {
	users := db.Table("users")
	if users == nil {
		return nil
	}
	row, err := users.Get(userID)
	if err != nil || row == nil {
		return nil
	}
	pub := ToUserPublic(row)

	profile := &UserProfile{
		UserPublic: *pub,
	}

	// Count tweets using authorId index
	tweets := db.Table("tweets")
	if tweets != nil {
		profile.TweetCount = tweets.CountByIndex("authorId", userID)
	}

	// Count followers/following using indexes
	follows := db.Table("follows")
	if follows != nil {
		profile.FollowerCount = follows.CountByIndex("followingId", userID)
		profile.FollowingCount = follows.CountByIndex("followerId", userID)
		if viewerID != "" {
			// Check if viewer follows this user via composite key lookup
			followID := viewerID + ":" + userID
			row, err := follows.Get(followID)
			profile.IsFollowing = err == nil && row != nil
		}
	}

	return profile
}

// FindUserByHandle looks up a user by their unique handle.
func FindUserByHandle(db *flop.Database, handle string) map[string]any {
	users := db.Table("users")
	if users == nil {
		return nil
	}
	row, ok := users.FindByUniqueIndex("handle", handle)
	if !ok {
		return nil
	}
	return row
}

// GetNotifications returns notifications for a user.
func GetNotifications(db *flop.Database, userID string, limit, offset int) []map[string]any {
	notifs := db.Table("notifications")
	if notifs == nil {
		return nil
	}

	userNotifs, err := notifs.FindByIndex("userId", userID)
	if err != nil {
		return nil
	}

	sort.Slice(userNotifs, func(i, j int) bool {
		return Num(userNotifs[i]["createdAt"]) > Num(userNotifs[j]["createdAt"])
	})

	if offset >= len(userNotifs) {
		return nil
	}
	end := offset + limit
	if end > len(userNotifs) {
		end = len(userNotifs)
	}

	result := userNotifs[offset:end]

	// Enrich with actor info
	users := db.Table("users")
	for i, n := range result {
		if users != nil {
			actorRow, err := users.Get(Str(n["actorId"]))
			if err == nil && actorRow != nil {
				result[i]["actor"] = ToUserPublic(actorRow)
			}
		}
	}

	return result
}

// ResolveHead returns head metadata for SSR.
func ResolveHead(db *flop.Database, path string) HeadPayload {
	switch {
	case path == "/" || path == "/home":
		return HeadPayload{
			Title: "Home / Chirp",
			Meta: []HeadMeta{
				{Name: "description", Content: "See what's happening in the world right now."},
			},
		}
	case path == "/explore":
		return HeadPayload{
			Title: "Explore / Chirp",
			Meta: []HeadMeta{
				{Name: "description", Content: "Explore trending topics and discover new people."},
			},
		}
	case path == "/notifications":
		return HeadPayload{
			Title: "Notifications / Chirp",
		}
	case strings.HasPrefix(path, "/search"):
		return HeadPayload{
			Title: "Search / Chirp",
		}
	case strings.HasPrefix(path, "/tweet/"):
		tweetID := strings.TrimPrefix(path, "/tweet/")
		tweets := db.Table("tweets")
		if tweets != nil {
			row, err := tweets.Get(tweetID)
			if err == nil && row != nil {
				content := Str(row["content"])
				if len(content) > 100 {
					content = content[:100] + "..."
				}
				users := db.Table("users")
				authorName := "Someone"
				if users != nil {
					author, err := users.Get(Str(row["authorId"]))
					if err == nil && author != nil {
						authorName = Str(author["displayName"])
					}
				}
				return HeadPayload{
					Title: authorName + ": \"" + content + "\" / Chirp",
					Meta: []HeadMeta{
						{Name: "description", Content: content},
					},
				}
			}
		}
		return HeadPayload{Title: "Tweet / Chirp"}
	case strings.HasPrefix(path, "/"):
		handle, _ := url.PathUnescape(strings.TrimPrefix(path, "/"))
		if handle != "" && !strings.Contains(handle, "/") {
			user := FindUserByHandle(db, handle)
			if user != nil {
				displayName := Str(user["displayName"])
				bio := Str(user["bio"])
				return HeadPayload{
					Title: fmt.Sprintf("%s (@%s) / Chirp", displayName, handle),
					Meta: []HeadMeta{
						{Name: "description", Content: bio},
					},
				}
			}
		}
		return HeadPayload{Title: "Chirp"}
	default:
		return HeadPayload{Title: "Chirp"}
	}
}

// ---- helpers ----

func Str(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func Num(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	default:
		return 0
	}
}

func Integer(v any) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	case float32:
		return int(val)
	default:
		return 0
	}
}

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

type tableStore interface {
	Table(name string) *flop.TableInstance
}

type GetTimelineIn struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

type GetTweetIn struct {
	TweetID string `json:"tweetId"`
}

type GetTweetRepliesIn struct {
	TweetID string `json:"tweetId"`
	Limit   int    `json:"limit"`
	Offset  int    `json:"offset"`
}

type CreateTweetIn struct {
	Content   string `json:"content"`
	ReplyToID string `json:"replyToId"`
	QuoteOfID string `json:"quoteOfId"`
}

type ToggleLikeIn struct {
	TweetID string `json:"tweetId"`
}

type ToggleRetweetIn struct {
	TweetID string `json:"tweetId"`
}

type ToggleFollowIn struct {
	UserID string `json:"userId"`
}

type GetUserProfileIn struct {
	Handle string `json:"handle"`
}

type GetUserTweetsIn struct {
	Handle string `json:"handle"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

type SearchIn struct {
	Q     string `json:"q"`
	Type  string `json:"type"`
	Limit int    `json:"limit"`
}

type GetNotificationsIn struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

type GetStatsIn struct{}

type AuthRegisterIn struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	Handle      string `json:"handle"`
	DisplayName string `json:"displayName"`
}

type AuthLoginIn struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthMeIn struct{}

const JWTSecret = "chirp-dev-secret"

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
		EnablePprof:           true,
	})

	users := flop.Define(application, "users", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
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
		s.String("id").Primary("uuidv7")
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

	flop.Define(application, "likes", func(s *flop.SchemaBuilder) {
		s.Number("id").Primary("autoincrement")
		s.String("edgeKey").Required().Unique()
		s.Ref("userId", users, "id").Required()
		s.String("tweetId").Required().Index()
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.Define(application, "retweets", func(s *flop.SchemaBuilder) {
		s.Number("id").Primary("autoincrement")
		s.String("edgeKey").Required().Unique()
		s.Ref("userId", users, "id").Required()
		s.String("tweetId").Required().Index()
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.Define(application, "follows", func(s *flop.SchemaBuilder) {
		s.Number("id").Primary("autoincrement")
		s.String("edgeKey").Required().Unique()
		s.Ref("followerId", users, "id").Required().Index()
		s.Ref("followingId", users, "id").Required().Index()
		s.Timestamp("createdAt").DefaultNow()
	})

	// Notifications
	flop.Define(application, "notifications", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.Ref("userId", users, "id").Required().Index() // who receives
		s.Ref("actorId", users, "id").Required()        // who caused
		s.Enum("type", "like", "retweet", "reply", "follow", "quote").Required()
		s.String("tweetId") // related tweet (if applicable)
		s.Boolean("read").Default(false)
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.View(application, "get_timeline", flop.Public(), GetTimelineView)
	flop.View(application, "get_tweet", flop.Public(), GetTweetView)
	flop.View(application, "get_tweet_replies", flop.Public(), GetTweetRepliesView)
	flop.View(application, "get_user_profile", flop.Public(), GetUserProfileView)
	flop.View(application, "get_user_tweets", flop.Public(), GetUserTweetsView)
	flop.View(application, "search", flop.Public(), SearchView)
	flop.View(application, "get_notifications", flop.Authenticated(), GetNotificationsView)
	flop.View(application, "get_stats", flop.Public(), GetStatsView)
	flop.View(application, "auth_me", flop.Authenticated(), AuthMeView)

	flop.Reducer(application, "create_tweet", flop.Authenticated(), CreateTweetReducer)
	flop.Reducer(application, "toggle_like", flop.Authenticated(), ToggleLikeReducer)
	flop.Reducer(application, "toggle_retweet", flop.Authenticated(), ToggleRetweetReducer)
	flop.Reducer(application, "toggle_follow", flop.Authenticated(), ToggleFollowReducer)
	flop.Reducer(application, "auth_register", flop.Public(), AuthRegisterReducer)
	flop.Reducer(application, "auth_login", flop.Public(), AuthLoginReducer)

	return application
}

func GetTimelineView(ctx *flop.ViewCtx, in GetTimelineIn) ([]*TweetWithAuthor, error) {
	limit := clampInt(in.Limit, 1, 200, 50)
	offset := in.Offset
	if offset < 0 {
		offset = 0
	}
	return ListTimeline(ctx.DB, limit, offset, viewerID(ctx.Request.Auth)), nil
}

func GetTweetView(ctx *flop.ViewCtx, in GetTweetIn) (*TweetWithAuthor, error) {
	tweetID := strings.TrimSpace(in.TweetID)
	if tweetID == "" {
		return nil, fmt.Errorf("tweetId is required")
	}
	tweets := ctx.DB.Table("tweets")
	if tweets == nil {
		return nil, fmt.Errorf("tweets table not found")
	}
	row, err := tweets.Get(tweetID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return ToTweetWithAuthor(row, ctx.DB, viewerID(ctx.Request.Auth)), nil
}

func GetTweetRepliesView(ctx *flop.ViewCtx, in GetTweetRepliesIn) ([]*TweetWithAuthor, error) {
	tweetID := strings.TrimSpace(in.TweetID)
	if tweetID == "" {
		return nil, fmt.Errorf("tweetId is required")
	}
	limit := clampInt(in.Limit, 1, 200, 50)
	offset := in.Offset
	if offset < 0 {
		offset = 0
	}
	return ListReplies(ctx.DB, tweetID, limit, offset, viewerID(ctx.Request.Auth)), nil
}

func GetUserProfileView(ctx *flop.ViewCtx, in GetUserProfileIn) (*UserProfile, error) {
	handle := strings.ToLower(strings.TrimSpace(in.Handle))
	if handle == "" {
		return nil, fmt.Errorf("handle is required")
	}
	userRow := FindUserByHandle(ctx.DB, handle)
	if userRow == nil {
		return nil, nil
	}
	userID := Str(userRow["id"])
	return GetUserProfile(ctx.DB, userID, viewerID(ctx.Request.Auth)), nil
}

func GetUserTweetsView(ctx *flop.ViewCtx, in GetUserTweetsIn) ([]*TweetWithAuthor, error) {
	handle := strings.ToLower(strings.TrimSpace(in.Handle))
	if handle == "" {
		return nil, fmt.Errorf("handle is required")
	}
	limit := clampInt(in.Limit, 1, 200, 50)
	offset := in.Offset
	if offset < 0 {
		offset = 0
	}
	userRow := FindUserByHandle(ctx.DB, handle)
	if userRow == nil {
		return []*TweetWithAuthor{}, nil
	}
	return ListUserTweets(ctx.DB, Str(userRow["id"]), limit, offset, viewerID(ctx.Request.Auth)), nil
}

func SearchView(ctx *flop.ViewCtx, in SearchIn) (map[string]any, error) {
	q := strings.TrimSpace(in.Q)
	if q == "" {
		return map[string]any{"tweets": []any{}, "users": []any{}}, nil
	}
	limit := clampInt(in.Limit, 1, 50, 20)
	searchType := strings.ToLower(strings.TrimSpace(in.Type))
	viewer := viewerID(ctx.Request.Auth)
	result := map[string]any{}
	if searchType == "" || searchType == "tweets" {
		result["tweets"] = SearchTweets(ctx.DB, q, limit, viewer)
	}
	if searchType == "" || searchType == "users" {
		result["users"] = SearchUsersScan(ctx.DB, q, limit)
	}
	return result, nil
}

func GetNotificationsView(ctx *flop.ViewCtx, in GetNotificationsIn) ([]map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	limit := clampInt(in.Limit, 1, 200, 50)
	offset := in.Offset
	if offset < 0 {
		offset = 0
	}
	return GetNotifications(ctx.DB, auth.ID, limit, offset), nil
}

func GetStatsView(ctx *flop.ViewCtx, _ GetStatsIn) (map[string]any, error) {
	stats := map[string]any{}
	for _, name := range []string{"users", "tweets", "likes", "retweets", "follows", "notifications"} {
		t := ctx.DB.Table(name)
		if t != nil {
			stats[name] = t.Count()
		}
	}
	return stats, nil
}

func AuthMeView(ctx *flop.ViewCtx, _ AuthMeIn) (*UserPublic, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	users := ctx.DB.Table("users")
	if users == nil {
		return nil, fmt.Errorf("users table not found")
	}
	row, err := users.Get(auth.ID)
	if err != nil || row == nil {
		return nil, fmt.Errorf("user not found")
	}
	return ToUserPublic(row), nil
}

func AuthRegisterReducer(ctx *flop.ReducerCtx, in AuthRegisterIn) (map[string]any, error) {
	email := strings.TrimSpace(in.Email)
	password := strings.TrimSpace(in.Password)
	handle := strings.ToLower(strings.TrimSpace(in.Handle))
	displayName := strings.TrimSpace(in.DisplayName)
	if email == "" || password == "" || handle == "" || displayName == "" {
		return nil, fmt.Errorf("all fields required")
	}
	users := ctx.DB.Table("users")
	if users == nil {
		return nil, fmt.Errorf("users table not found")
	}
	hashedPw, err := flop.HashPassword(password)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password")
	}
	row, err := users.Insert(map[string]any{
		"email":       email,
		"password":    hashedPw,
		"handle":      handle,
		"displayName": displayName,
		"roles":       []any{"user"},
	})
	if err != nil {
		return nil, err
	}
	roles := rolesFromRow(row)
	if len(roles) == 0 {
		roles = []string{"user"}
	}
	token := flop.CreateJWT(&flop.JWTPayload{
		Sub:   Str(row["id"]),
		Email: Str(row["email"]),
		Roles: roles,
		Exp:   time.Now().Add(24 * time.Hour).Unix(),
	}, JWTSecret)
	return map[string]any{"token": token, "user": ToUserPublic(row)}, nil
}

func AuthLoginReducer(ctx *flop.ReducerCtx, in AuthLoginIn) (map[string]any, error) {
	email := strings.TrimSpace(in.Email)
	password := in.Password
	if email == "" || password == "" {
		return nil, fmt.Errorf("email and password required")
	}
	users := ctx.DB.Table("users")
	if users == nil {
		return nil, fmt.Errorf("users table not found")
	}
	row, ok := users.FindByUniqueIndex("email", email)
	if !ok || row == nil {
		return nil, fmt.Errorf("invalid credentials")
	}
	if !flop.VerifyPassword(password, Str(row["password"])) {
		return nil, fmt.Errorf("invalid credentials")
	}
	roles := rolesFromRow(row)
	if len(roles) == 0 {
		roles = []string{"user"}
	}
	token := flop.CreateJWT(&flop.JWTPayload{
		Sub:   Str(row["id"]),
		Email: Str(row["email"]),
		Roles: roles,
		Exp:   time.Now().Add(24 * time.Hour).Unix(),
	}, JWTSecret)
	return map[string]any{"token": token, "user": ToUserPublic(row)}, nil
}

func CreateTweetReducer(ctx *flop.ReducerCtx, in CreateTweetIn) (*TweetWithAuthor, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	content := strings.TrimSpace(in.Content)
	if content == "" || len(content) > 280 {
		return nil, fmt.Errorf("content must be 1-280 characters")
	}
	tweets := ctx.DB.Table("tweets")
	if tweets == nil {
		return nil, fmt.Errorf("tweets table not found")
	}
	data := map[string]any{
		"authorId": auth.ID,
		"content":  content,
	}
	replyToID := strings.TrimSpace(in.ReplyToID)
	quoteOfID := strings.TrimSpace(in.QuoteOfID)
	if replyToID != "" {
		data["replyToId"] = replyToID
	}
	if quoteOfID != "" {
		data["quoteOfId"] = quoteOfID
	}
	row, err := tweets.Insert(data)
	if err != nil {
		return nil, err
	}
	tweetID := Str(row["id"])
	if replyToID != "" {
		parent, err := tweets.Get(replyToID)
		if err == nil && parent != nil {
			parentAuthor := Str(parent["authorId"])
			if parentAuthor != auth.ID {
				if notifs := ctx.DB.Table("notifications"); notifs != nil {
					_, _ = notifs.Insert(map[string]any{
						"userId":  parentAuthor,
						"actorId": auth.ID,
						"type":    "reply",
						"tweetId": tweetID,
					})
				}
			}
		}
	}
	if quoteOfID != "" {
		quoted, err := tweets.Get(quoteOfID)
		if err == nil && quoted != nil {
			quotedAuthor := Str(quoted["authorId"])
			if quotedAuthor != auth.ID {
				if notifs := ctx.DB.Table("notifications"); notifs != nil {
					_, _ = notifs.Insert(map[string]any{
						"userId":  quotedAuthor,
						"actorId": auth.ID,
						"type":    "quote",
						"tweetId": tweetID,
					})
				}
			}
		}
	}
	return ToTweetWithAuthor(row, ctx.DB, auth.ID), nil
}

func ToggleLikeReducer(ctx *flop.ReducerCtx, in ToggleLikeIn) (map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	tweetID := strings.TrimSpace(in.TweetID)
	if tweetID == "" {
		return nil, fmt.Errorf("tweetId is required")
	}
	likes := ctx.DB.Table("likes")
	if likes == nil {
		return nil, fmt.Errorf("likes table not found")
	}
	edgeKey := auth.ID + ":" + tweetID
	existing, ok := likes.FindByUniqueIndex("edgeKey", edgeKey)
	if ok && existing != nil {
		_, err := likes.Delete(Str(existing["id"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"liked": false}, nil
	}
	if _, err := likes.Insert(map[string]any{"edgeKey": edgeKey, "userId": auth.ID, "tweetId": tweetID}); err != nil {
		return nil, err
	}
	if tweets := ctx.DB.Table("tweets"); tweets != nil {
		tweet, err := tweets.Get(tweetID)
		if err == nil && tweet != nil {
			tweetAuthor := Str(tweet["authorId"])
			if tweetAuthor != auth.ID {
				if notifs := ctx.DB.Table("notifications"); notifs != nil {
					_, _ = notifs.Insert(map[string]any{
						"userId":  tweetAuthor,
						"actorId": auth.ID,
						"type":    "like",
						"tweetId": tweetID,
					})
				}
			}
		}
	}
	return map[string]any{"liked": true}, nil
}

func ToggleRetweetReducer(ctx *flop.ReducerCtx, in ToggleRetweetIn) (map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	tweetID := strings.TrimSpace(in.TweetID)
	if tweetID == "" {
		return nil, fmt.Errorf("tweetId is required")
	}
	retweets := ctx.DB.Table("retweets")
	if retweets == nil {
		return nil, fmt.Errorf("retweets table not found")
	}
	edgeKey := auth.ID + ":" + tweetID
	existing, ok := retweets.FindByUniqueIndex("edgeKey", edgeKey)
	if ok && existing != nil {
		_, err := retweets.Delete(Str(existing["id"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"retweeted": false}, nil
	}
	if _, err := retweets.Insert(map[string]any{"edgeKey": edgeKey, "userId": auth.ID, "tweetId": tweetID}); err != nil {
		return nil, err
	}
	if tweets := ctx.DB.Table("tweets"); tweets != nil {
		tweet, err := tweets.Get(tweetID)
		if err == nil && tweet != nil {
			tweetAuthor := Str(tweet["authorId"])
			if tweetAuthor != auth.ID {
				if notifs := ctx.DB.Table("notifications"); notifs != nil {
					_, _ = notifs.Insert(map[string]any{
						"userId":  tweetAuthor,
						"actorId": auth.ID,
						"type":    "retweet",
						"tweetId": tweetID,
					})
				}
			}
		}
	}
	return map[string]any{"retweeted": true}, nil
}

func ToggleFollowReducer(ctx *flop.ReducerCtx, in ToggleFollowIn) (map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	followingID := strings.TrimSpace(in.UserID)
	if followingID == "" || followingID == auth.ID {
		return nil, fmt.Errorf("invalid follow target")
	}
	follows := ctx.DB.Table("follows")
	if follows == nil {
		return nil, fmt.Errorf("follows table not found")
	}
	edgeKey := auth.ID + ":" + followingID
	existing, ok := follows.FindByUniqueIndex("edgeKey", edgeKey)
	if ok && existing != nil {
		_, err := follows.Delete(Str(existing["id"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"following": false}, nil
	}
	if _, err := follows.Insert(map[string]any{
		"edgeKey":     edgeKey,
		"followerId":  auth.ID,
		"followingId": followingID,
	}); err != nil {
		return nil, err
	}
	if notifs := ctx.DB.Table("notifications"); notifs != nil {
		_, _ = notifs.Insert(map[string]any{
			"userId":  followingID,
			"actorId": auth.ID,
			"type":    "follow",
		})
	}
	return map[string]any{"following": true}, nil
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

func ToTweetWithAuthor(row map[string]any, db tableStore, viewerID string) *TweetWithAuthor {
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
			row, ok := likes.FindByUniqueIndex("edgeKey", viewerID+":"+t.ID)
			t.Liked = ok && row != nil
		}
		retweets := db.Table("retweets")
		if retweets != nil {
			row, ok := retweets.FindByUniqueIndex("edgeKey", viewerID+":"+t.ID)
			t.Retweeted = ok && row != nil
		}
	}

	return t
}

// ListTimeline returns tweets for the home feed (all tweets, newest first).
func ListTimeline(db tableStore, limit, offset int, viewerID string) []*TweetWithAuthor {
	tweets := db.Table("tweets")
	if tweets == nil {
		return nil
	}

	// Read from the newest tail in chunks, not from the oldest head.
	// Scanning from offset 0 misses recent rows on large datasets.
	total := tweets.Count()
	chunk := limit*4 + offset*2 + 500
	if chunk < 2000 {
		chunk = 2000
	}
	if chunk > total {
		chunk = total
	}
	start := total - chunk
	if start < 0 {
		start = 0
	}

	var filtered []map[string]any
	target := offset + limit
	if target < 100 {
		target = 100
	}
	for {
		rows, err := tweets.Scan(chunk, start)
		if err != nil {
			return nil
		}
		for _, row := range rows {
			if Str(row["replyToId"]) == "" {
				filtered = append(filtered, row)
			}
		}
		if start == 0 || len(filtered) >= target {
			break
		}
		if start <= chunk {
			start = 0
		} else {
			start -= chunk
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
func ListUserTweets(db tableStore, userID string, limit, offset int, viewerID string) []*TweetWithAuthor {
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
func ListReplies(db tableStore, tweetID string, limit, offset int, viewerID string) []*TweetWithAuthor {
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
func SearchTweets(db tableStore, query string, limit int, viewerID string) []*TweetWithAuthor {
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
func SearchUsers(db tableStore, autocomplete *flop.AutocompleteIndex, query string, limit int) []*UserPublic {
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

func SearchUsersScan(db tableStore, query string, limit int) []*UserPublic {
	users := db.Table("users")
	if users == nil {
		return nil
	}
	rows, err := users.Scan(10000, 0)
	if err != nil {
		return nil
	}
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return []*UserPublic{}
	}
	result := make([]*UserPublic, 0, limit)
	for _, row := range rows {
		handle := strings.ToLower(Str(row["handle"]))
		display := strings.ToLower(Str(row["displayName"]))
		if !strings.Contains(handle, q) && !strings.Contains(display, q) {
			continue
		}
		result = append(result, ToUserPublic(row))
		if len(result) >= limit {
			break
		}
	}
	return result
}

// GetUserProfile builds a full profile with counts.
func GetUserProfile(db tableStore, userID string, viewerID string) *UserProfile {
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
			row, ok := follows.FindByUniqueIndex("edgeKey", viewerID+":"+userID)
			profile.IsFollowing = ok && row != nil
		}
	}

	return profile
}

// FindUserByHandle looks up a user by their unique handle.
func FindUserByHandle(db tableStore, handle string) map[string]any {
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
func GetNotifications(db tableStore, userID string, limit, offset int) []map[string]any {
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
func ResolveHead(db tableStore, path string) HeadPayload {
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

func rolesFromRow(row map[string]any) []string {
	raw, ok := row["roles"]
	if !ok || raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, role := range v {
			role = strings.TrimSpace(role)
			if role != "" {
				out = append(out, role)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			role := strings.TrimSpace(fmt.Sprintf("%v", item))
			if role != "" {
				out = append(out, role)
			}
		}
		return out
	default:
		role := strings.TrimSpace(fmt.Sprintf("%v", raw))
		if role == "" {
			return nil
		}
		return []string{role}
	}
}

func viewerID(auth *flop.AuthContext) string {
	if auth == nil {
		return ""
	}
	return auth.ID
}

func clampInt(v, lo, hi, fallback int) int {
	if v == 0 {
		v = fallback
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

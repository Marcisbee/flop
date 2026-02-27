package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/marcisbee/flop"
	"github.com/marcisbee/flop/examples/twitter-go-react/app"
)

var firstNames = []string{
	"Olivia", "Liam", "Emma", "Noah", "Ava", "Elijah", "Sophia", "James",
	"Isabella", "William", "Mia", "Benjamin", "Charlotte", "Lucas", "Amelia",
	"Henry", "Harper", "Alexander", "Evelyn", "Daniel", "Luna", "Michael",
	"Ella", "Mason", "Chloe", "Ethan", "Penelope", "Sebastian", "Layla",
	"Jack", "Riley", "Aiden", "Zoey", "Owen", "Nora", "Samuel", "Lily",
	"Ryan", "Eleanor", "Nathan", "Hannah", "Caleb", "Lillian", "Dylan",
	"Addison", "Luke", "Aubrey", "Andrew", "Ellie", "Isaac", "Stella",
	"Jayden", "Natalie", "Leo", "Zoe", "Gabriel", "Leah", "Anthony",
	"Hazel", "Jaxon", "Violet", "Lincoln", "Aurora", "Joshua", "Savannah",
	"Christopher", "Audrey", "Ezra", "Brooklyn", "Mateo", "Bella", "Charles",
	"Claire", "Thomas", "Skylar", "John", "Lucy", "David", "Paisley",
	"Carter", "Everly", "Wyatt", "Anna", "Julian", "Caroline", "Asher",
	"Nova", "Grayson", "Genesis", "Levi", "Emilia", "Jackson", "Kennedy",
	"Isaiah", "Maya", "Christian", "Willow", "Muhammad", "Kinsley", "Landon",
	"Naomi", "Josiah", "Aaliyah", "Jonathan", "Elena", "Maverick", "Sarah",
	"Nolan", "Ariana", "Hunter", "Allison", "Connor", "Gabriella", "Adrian",
}

var lastNames = []string{
	"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
	"Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
	"Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
	"Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
	"Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
	"Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
	"Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
	"Carter", "Roberts", "Park", "Kim", "Chen", "Wu", "Patel",
	"Shah", "Kumar", "Singh", "Cohen", "Berg", "Muller", "Wagner",
	"Fischer", "Weber", "Schneider", "Meyer", "Wolf", "Bauer", "Koch",
	"Richter", "Klein", "Schroeder", "Neumann", "Schwartz", "Braun",
	"Hofmann", "Hartmann", "Krause", "Werner", "Schmitt", "Meier",
	"Lehmann", "Schmid", "Schulze", "Maier", "Otto", "Peters", "Sato",
	"Suzuki", "Takahashi", "Tanaka", "Watanabe", "Ito", "Yamamoto",
	"Nakamura", "Kobayashi", "Kato", "Rossi", "Russo", "Ferrari",
}

var bios = []string{
	"Software engineer by day, cat whisperer by night",
	"Building the future one commit at a time",
	"Coffee enthusiast | Code addict | Dog lover",
	"Full-stack developer with a passion for clean code",
	"Dreaming in TypeScript, waking up to Go",
	"Open source contributor | Mentor | Speaker",
	"Just a developer trying to make the web a better place",
	"Turning caffeine into code since 2010",
	"Cloud architect | DevOps | Kubernetes fanatic",
	"Design systems engineer @ a startup you'll hear about soon",
	"Making pixels dance since forever",
	"Functional programming advocate",
	"AI/ML researcher exploring the boundaries",
	"Product designer who codes. Sometimes the other way around.",
	"Writing code and stories. Both have bugs.",
	"Startup founder. Third time's the charm.",
	"Ex-FAANG, now building my own thing",
	"Digital nomad writing code from around the world",
	"Teaching machines to think. Still learning myself.",
	"Rust evangelist. Yes, I'll tell you about it.",
	"Building developer tools nobody asked for",
	"Data engineer | Python | SQL | Kafka",
	"Frontend architect | React | Performance geek",
	"Backend developer | Go | PostgreSQL | Redis",
	"DevRel at heart, engineer by trade",
	"Making databases go brrr",
	"Security researcher. Don't hack me.",
	"Mobile dev building apps people actually use",
	"Platform engineer making deploys boring (the good kind)",
	"Indie hacker building in public",
}

var tweetTemplates = []string{
	"Just shipped a new feature! %s",
	"TIL: %s",
	"Hot take: %s is overrated",
	"Can we talk about how %s changed everything?",
	"Working on something exciting with %s",
	"The best thing about %s is the community",
	"Anyone else struggling with %s today?",
	"Unpopular opinion: %s",
	"Just discovered %s and it's amazing",
	"The future of %s looks incredibly promising",
	"Debugging %s for 3 hours. It was a missing semicolon.",
	"My %s setup is finally perfect. Time to change it again.",
	"Reading about %s and my mind is blown",
	"Shipped to production on a Friday. Living dangerously with %s",
	"The %s docs are actually really good now",
	"Started learning %s today. Wish me luck!",
	"Hot take: %s will be the next big thing",
	"Spent the weekend building a %s project. Worth it.",
	"The performance gains from %s are insane",
	"Migrating from %s to something better. Recommendations?",
}

var techTopics = []string{
	"React", "Go", "Rust", "TypeScript", "Python", "Kubernetes", "Docker",
	"GraphQL", "REST APIs", "WebAssembly", "serverless", "microservices",
	"AI/ML", "LLMs", "edge computing", "Web3", "databases", "Redis",
	"PostgreSQL", "SQLite", "Node.js", "Deno", "Bun", "Svelte", "Vue",
	"Next.js", "Remix", "Astro", "Tailwind CSS", "CSS Grid", "WebSockets",
	"gRPC", "Kafka", "RabbitMQ", "Terraform", "AWS", "GCP", "Azure",
	"CI/CD", "testing", "TDD", "observability", "OpenTelemetry", "Prometheus",
	"monorepos", "nx", "turborepo", "pnpm", "esbuild", "Vite", "webpack",
	"ESLint", "Prettier", "Biome", "htmx", "Alpine.js", "Solid.js",
	"Qwik", "Nuxt", "SvelteKit", "Hono", "Fastify", "Express", "Gin",
	"Fiber", "Echo", "chi", "net/http", "SQLx", "Drizzle", "Prisma",
}

var thoughts = []string{
	"The best code is the code you don't write",
	"Premature optimization is the root of all evil, but lazy optimization is the root of all slowness",
	"If it works, don't touch it. If it doesn't work, rewrite it from scratch.",
	"Documentation is like exercise. Everyone knows they should do more of it.",
	"The only thing worse than no tests is flaky tests",
	"Code reviews are where friendships go to die",
	"There are only two hard things in CS: cache invalidation and naming things",
	"The best meetings are the ones that get cancelled",
	"Working from home means my commute is 10 steps but somehow I'm still late",
	"Nothing motivates me to clean my code like someone saying they want to look at it",
	"Every developer's favorite lie: I'll add tests later",
	"My code works and I have no idea why",
	"My code doesn't work and I have no idea why",
	"Just one more feature before we launch. Said 47 features ago.",
	"The best debugging tool is still a good night's sleep",
	"Copy-pasting from Stack Overflow is called research",
	"I don't always test my code, but when I do, I do it in production",
	"Software is like entropy: it always increases",
	"The first 90% of the code takes 90% of the time. The other 10% takes the other 90%.",
	"It works on my machine. Ship my machine.",
	"Good morning! Time to mass produce some bugs",
	"Can't have technical debt if you never plan to pay it back",
	"The cloud is just someone else's computer having the same problems",
	"Refactoring: the art of breaking working code in new and exciting ways",
	"A good developer is a lazy developer. Fight me.",
	"Just realized my git history reads like a crime novel",
	"TODO comments are just hopes and dreams in code form",
	"Why use a framework when you can build your own worse version?",
	"CSS is the dark magic of web development",
	"The real full stack was the bugs we made along the way",
	"Just finished a 2 hour meeting that could have been a Slack message",
	"Day 47 of pretending I understand what the codebase does",
	"Sometimes I wonder if my code compiles because of skill or luck",
	"404: motivation not found",
	"Tabs vs spaces? I use both because I like chaos",
	"Just pushed directly to main. What could go wrong?",
	"Algorithm interviews: where knowing how to reverse a linked list matters more than building products",
	"The best part of being a developer is explaining to non-devs what you do",
	"Started a side project. It'll be done never.",
	"If Stack Overflow goes down, half the internet goes with it",
}

var replyTemplates = []string{
	"This is so true!",
	"Couldn't agree more",
	"Great point. Have you also considered %s?",
	"I had the same experience with %s",
	"Thanks for sharing this!",
	"This is exactly what I needed to hear today",
	"Interesting perspective. I think %s is also worth considering",
	"Been saying this for years",
	"Hard agree on this one",
	"100%% this",
	"I've been working with %s and this resonates",
	"Bookmarking this for later",
	"This thread is gold",
	"Someone finally said it",
	"Wait, you guys are getting %s to work?",
	"The real MVP right here",
	"Strong disagree, but I respect the take",
	"Can you elaborate on the %s part?",
	"This changed my perspective on %s",
	"Adding this to my reading list",
}

var locations = []string{
	"San Francisco, CA", "New York, NY", "London, UK", "Berlin, Germany",
	"Tokyo, Japan", "Toronto, Canada", "Sydney, Australia", "Paris, France",
	"Amsterdam, Netherlands", "Stockholm, Sweden", "Singapore", "Seoul, South Korea",
	"Austin, TX", "Seattle, WA", "Portland, OR", "Denver, CO", "Chicago, IL",
	"Los Angeles, CA", "Boston, MA", "Miami, FL", "Lisbon, Portugal",
	"Barcelona, Spain", "Dublin, Ireland", "Zurich, Switzerland",
	"Copenhagen, Denmark", "Helsinki, Finland", "Remote",
	"Everywhere and nowhere", "The internet", "127.0.0.1",
}

func main() {
	var (
		force     bool
		profiles  int
		tweets    int
		batchSize int
	)
	flag.BoolVar(&force, "force", false, "remove existing data before seeding")
	flag.IntVar(&profiles, "profiles", 10000, "number of user profiles to create")
	flag.IntVar(&tweets, "tweets", 500000, "number of tweets to create")
	flag.IntVar(&batchSize, "batch", 2000, "batch size for bulk inserts")
	flag.Parse()

	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatalf("seed: find module root: %v", err)
	}

	dataDir := filepath.Join(projectRoot, "data")
	if force {
		log.Printf("seed: removing existing data at %s", dataDir)
		os.RemoveAll(dataDir)
	}
	os.MkdirAll(dataDir, 0o755)

	application := app.BuildWithDataDir(dataDir)
	db, err := application.Open()
	if err != nil {
		log.Fatalf("seed: open database: %v", err)
	}
	defer func() {
		_ = db.Checkpoint()
		_ = db.Close()
	}()

	usersTable := db.Table("users")
	tweetsTable := db.Table("tweets")
	likesTable := db.Table("likes")
	retweetsTable := db.Table("retweets")
	followsTable := db.Table("follows")

	if !force && usersTable.Count() > 0 {
		log.Fatalf("seed: database already has %d users (use -force to reset)", usersTable.Count())
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	started := time.Now()

	// ---- Seed users ----
	log.Printf("seed: creating %d user profiles...", profiles)
	hashedPw, err := flop.HashPassword("password")
	if err != nil {
		log.Fatalf("seed: hash password: %v", err)
	}

	userIDs := make([]string, 0, profiles)
	userBatch := make([]map[string]any, 0, batchSize)
	handleSet := make(map[string]bool)

	for i := 0; i < profiles; i++ {
		first := firstNames[rng.Intn(len(firstNames))]
		last := lastNames[rng.Intn(len(lastNames))]
		handle := strings.ToLower(first) + strings.ToLower(last) + fmt.Sprintf("%d", rng.Intn(9999))
		for handleSet[handle] {
			handle = strings.ToLower(first) + strings.ToLower(last) + fmt.Sprintf("%d", rng.Intn(99999))
		}
		handleSet[handle] = true

		bio := ""
		if rng.Float32() < 0.8 {
			bio = bios[rng.Intn(len(bios))]
		}
		location := ""
		if rng.Float32() < 0.6 {
			location = locations[rng.Intn(len(locations))]
		}

		avatarBg := fmt.Sprintf("%02x%02x%02x", rng.Intn(200)+56, rng.Intn(200)+56, rng.Intn(200)+56)
		avatarUrl := fmt.Sprintf("https://ui-avatars.com/api/?name=%s+%s&background=%s&color=fff&size=128&bold=true", first, last, avatarBg)

		user := map[string]any{
			"email":       fmt.Sprintf("%s@example.com", handle),
			"password":    hashedPw,
			"handle":      handle,
			"displayName": first + " " + last,
			"bio":         bio,
			"avatarUrl":   avatarUrl,
			"location":    location,
			"roles":       []any{"user"},
		}

		userBatch = append(userBatch, user)
		if len(userBatch) >= batchSize {
			n, err := usersTable.InsertMany(userBatch, len(userBatch))
			if err != nil {
				log.Printf("seed: user batch insert error: %v (inserted %d)", err, n)
			}
			// Collect IDs
			for _, u := range userBatch[:n] {
				if id, ok := u["id"].(string); ok {
					userIDs = append(userIDs, id)
				}
			}
			userBatch = userBatch[:0]
		}

		if (i+1)%5000 == 0 {
			log.Printf("seed: created %d/%d users (%.1fs)", i+1, profiles, time.Since(started).Seconds())
		}
	}
	if len(userBatch) > 0 {
		n, err := usersTable.InsertMany(userBatch, len(userBatch))
		if err != nil {
			log.Printf("seed: user batch insert error: %v (inserted %d)", err, n)
		}
		for _, u := range userBatch[:n] {
			if id, ok := u["id"].(string); ok {
				userIDs = append(userIDs, id)
			}
		}
	}

	// If InsertMany doesn't fill IDs in the batch, scan them
	if len(userIDs) == 0 {
		allUsers, _ := usersTable.Scan(profiles+100, 0)
		for _, u := range allUsers {
			if id := app.Str(u["id"]); id != "" {
				userIDs = append(userIDs, id)
			}
		}
	}

	log.Printf("seed: created %d users in %.1fs", len(userIDs), time.Since(started).Seconds())

	// ---- Seed follows ----
	followCount := profiles * 20 // average 20 follows per user
	if followCount > 200000 {
		followCount = 200000
	}
	log.Printf("seed: creating %d follow relationships...", followCount)
	followBatch := make([]map[string]any, 0, batchSize)
	followSet := make(map[string]bool)

	for i := 0; i < followCount; i++ {
		followerIdx := rng.Intn(len(userIDs))
		followingIdx := rng.Intn(len(userIDs))
		if followerIdx == followingIdx {
			continue
		}
		followerID := userIDs[followerIdx]
		followingID := userIDs[followingIdx]
		key := followerID + ":" + followingID
		if followSet[key] {
			continue
		}
		followSet[key] = true

		followBatch = append(followBatch, map[string]any{
			"id":          key,
			"followerId":  followerID,
			"followingId": followingID,
		})
		if len(followBatch) >= batchSize {
			n, err := followsTable.InsertMany(followBatch, len(followBatch))
			if err != nil {
				log.Printf("seed: follow batch error: %v (inserted %d)", err, n)
			}
			followBatch = followBatch[:0]
		}
	}
	if len(followBatch) > 0 {
		followsTable.InsertMany(followBatch, len(followBatch))
	}
	log.Printf("seed: follows created in %.1fs", time.Since(started).Seconds())

	// ---- Seed tweets ----
	log.Printf("seed: creating %d tweets...", tweets)
	tweetIDs := make([]string, 0, tweets)
	tweetBatch := make([]map[string]any, 0, batchSize)

	// Timestamp range: last 90 days
	now := time.Now()
	ninetyDaysAgo := now.Add(-90 * 24 * time.Hour)

	for i := 0; i < tweets; i++ {
		authorID := userIDs[rng.Intn(len(userIDs))]
		content := generateTweet(rng)
		ts := ninetyDaysAgo.Add(time.Duration(rng.Int63n(int64(now.Sub(ninetyDaysAgo)))))

		tweet := map[string]any{
			"authorId":  authorID,
			"content":   content,
			"createdAt": float64(ts.UnixMilli()),
		}

		// 15% chance of being a reply
		if rng.Float32() < 0.15 && len(tweetIDs) > 0 {
			replyToID := tweetIDs[rng.Intn(len(tweetIDs))]
			tweet["replyToId"] = replyToID
		}

		// 5% chance of being a quote tweet
		if rng.Float32() < 0.05 && len(tweetIDs) > 0 && tweet["replyToId"] == nil {
			quoteID := tweetIDs[rng.Intn(len(tweetIDs))]
			tweet["quoteOfId"] = quoteID
		}

		tweetBatch = append(tweetBatch, tweet)
		if len(tweetBatch) >= batchSize {
			n, err := tweetsTable.InsertMany(tweetBatch, len(tweetBatch))
			if err != nil {
				log.Printf("seed: tweet batch error: %v (inserted %d)", err, n)
			}
			tweetBatch = tweetBatch[:0]

			if (i+1)%50000 == 0 {
				log.Printf("seed: created %d/%d tweets (%.1fs)", i+1, tweets, time.Since(started).Seconds())
			}
		}
	}
	if len(tweetBatch) > 0 {
		tweetsTable.InsertMany(tweetBatch, len(tweetBatch))
	}

	// Collect all tweet IDs
	allTweets, _ := tweetsTable.Scan(tweets+100, 0)
	for _, t := range allTweets {
		if id := app.Str(t["id"]); id != "" {
			tweetIDs = append(tweetIDs, id)
		}
	}
	log.Printf("seed: created %d tweets in %.1fs", len(tweetIDs), time.Since(started).Seconds())

	// ---- Seed likes ----
	likeCount := tweets // roughly 1 like per tweet on average
	if likeCount > 500000 {
		likeCount = 500000
	}
	log.Printf("seed: creating ~%d likes...", likeCount)
	likeBatch := make([]map[string]any, 0, batchSize)
	likeSet := make(map[string]bool)

	for i := 0; i < likeCount; i++ {
		userID := userIDs[rng.Intn(len(userIDs))]
		tweetID := tweetIDs[rng.Intn(len(tweetIDs))]
		key := userID + ":" + tweetID
		if likeSet[key] {
			continue
		}
		likeSet[key] = true

		likeBatch = append(likeBatch, map[string]any{
			"id":      key,
			"userId":  userID,
			"tweetId": tweetID,
		})
		if len(likeBatch) >= batchSize {
			n, err := likesTable.InsertMany(likeBatch, len(likeBatch))
			if err != nil {
				log.Printf("seed: like batch error: %v (inserted %d)", err, n)
			}
			likeBatch = likeBatch[:0]
		}
	}
	if len(likeBatch) > 0 {
		likesTable.InsertMany(likeBatch, len(likeBatch))
	}
	log.Printf("seed: likes created in %.1fs", time.Since(started).Seconds())

	// ---- Seed retweets ----
	rtCount := tweets / 5 // 20% of tweets get retweeted
	if rtCount > 100000 {
		rtCount = 100000
	}
	log.Printf("seed: creating ~%d retweets...", rtCount)
	rtBatch := make([]map[string]any, 0, batchSize)
	rtSet := make(map[string]bool)

	for i := 0; i < rtCount; i++ {
		userID := userIDs[rng.Intn(len(userIDs))]
		tweetID := tweetIDs[rng.Intn(len(tweetIDs))]
		key := userID + ":" + tweetID
		if rtSet[key] {
			continue
		}
		rtSet[key] = true

		rtBatch = append(rtBatch, map[string]any{
			"id":      key,
			"userId":  userID,
			"tweetId": tweetID,
		})
		if len(rtBatch) >= batchSize {
			n, err := retweetsTable.InsertMany(rtBatch, len(rtBatch))
			if err != nil {
				log.Printf("seed: retweet batch error: %v (inserted %d)", err, n)
			}
			rtBatch = rtBatch[:0]
		}
	}
	if len(rtBatch) > 0 {
		retweetsTable.InsertMany(rtBatch, len(rtBatch))
	}
	log.Printf("seed: retweets created in %.1fs", time.Since(started).Seconds())

	// Note: likeCount, retweetCount, replyCount, quoteCount are cached fields
	// and will be automatically hydrated on next server start.

	log.Printf("seed: done! Total time: %.1fs", time.Since(started).Seconds())
	log.Printf("seed: users=%d tweets=%d likes=%d retweets=%d follows=%d",
		usersTable.Count(), tweetsTable.Count(), likesTable.Count(),
		retweetsTable.Count(), followsTable.Count())
}

func generateTweet(rng *rand.Rand) string {
	switch rng.Intn(4) {
	case 0:
		tmpl := tweetTemplates[rng.Intn(len(tweetTemplates))]
		topic := techTopics[rng.Intn(len(techTopics))]
		return fmt.Sprintf(tmpl, topic)
	case 1:
		return thoughts[rng.Intn(len(thoughts))]
	case 2:
		tmpl := replyTemplates[rng.Intn(len(replyTemplates))]
		topic := techTopics[rng.Intn(len(techTopics))]
		result := fmt.Sprintf(tmpl, topic)
		if len(result) > 280 {
			result = result[:280]
		}
		return result
	default:
		topic1 := techTopics[rng.Intn(len(techTopics))]
		topic2 := techTopics[rng.Intn(len(techTopics))]
		phrases := []string{
			fmt.Sprintf("Been using %s with %s and the DX is incredible", topic1, topic2),
			fmt.Sprintf("Just migrated from %s to %s. Here's what I learned:", topic1, topic2),
			fmt.Sprintf("%s + %s is an underrated combo", topic1, topic2),
			fmt.Sprintf("Day 1 of building a %s app with %s", topic1, topic2),
			fmt.Sprintf("The %s ecosystem keeps getting better. Especially with %s integration", topic1, topic2),
		}
		return phrases[rng.Intn(len(phrases))]
	}
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = next
	}
}

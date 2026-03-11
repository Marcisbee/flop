package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	flop "github.com/marcisbee/flop/go2"
)

func main() {
	force := false
	for _, arg := range os.Args[1:] {
		if arg == "-force" {
			force = true
		}
	}

	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatal(err)
	}
	dataDir := filepath.Join(projectRoot, "data")

	if force {
		os.RemoveAll(dataDir)
	}

	db, err := flop.OpenDB(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	authConfig := flop.DefaultAuthConfig()
	authConfig.TableName = "users"
	authMgr := flop.NewAuthManager(db, authConfig)

	// Create all tables
	schemas := []*flop.Schema{
		{
			Name: "users", IsAuth: true,
			Fields: []flop.Field{
				{Name: "email", Type: flop.FieldString, Required: true, Unique: true, MaxLen: 255},
				{Name: "password", Type: flop.FieldString, Required: true},
				{Name: "handle", Type: flop.FieldString, Required: true, Unique: true, MaxLen: 30},
				{Name: "display_name", Type: flop.FieldString, MaxLen: 50},
				{Name: "bio", Type: flop.FieldString, MaxLen: 500},
				{Name: "avatar", Type: flop.FieldString},
				{Name: "karma", Type: flop.FieldInt},
			},
		},
		{
			Name: "communities",
			Fields: []flop.Field{
				{Name: "name", Type: flop.FieldString, Required: true, MaxLen: 100},
				{Name: "handle", Type: flop.FieldString, Required: true, Unique: true, MaxLen: 30},
				{Name: "description", Type: flop.FieldString, Searchable: true, MaxLen: 1000},
				{Name: "rules", Type: flop.FieldString, MaxLen: 5000},
				{Name: "creator_id", Type: flop.FieldRef, RefTable: "users", Required: true},
				{Name: "member_count", Type: flop.FieldInt},
				{Name: "visibility", Type: flop.FieldString, EnumValues: []string{"public", "private", "restricted"}},
			},
		},
		{
			Name: "memberships",
			Fields: []flop.Field{
				{Name: "user_id", Type: flop.FieldRef, RefTable: "users", Required: true},
				{Name: "community_id", Type: flop.FieldRef, RefTable: "communities", Required: true},
				{Name: "role", Type: flop.FieldString, EnumValues: []string{"member", "moderator", "admin"}},
			},
			UniqueConstraints: [][]string{{"user_id", "community_id"}},
		},
		{
			Name: "posts",
			Fields: []flop.Field{
				{Name: "title", Type: flop.FieldString, Required: true, Searchable: true, MaxLen: 300},
				{Name: "body", Type: flop.FieldString, Searchable: true, MaxLen: 40000},
				{Name: "link", Type: flop.FieldString, MaxLen: 2048},
				{Name: "image", Type: flop.FieldString},
				{Name: "author_id", Type: flop.FieldRef, RefTable: "users", Required: true},
				{Name: "community_id", Type: flop.FieldRef, RefTable: "communities", Required: true},
				{Name: "score", Type: flop.FieldInt},
				{Name: "hot_rank", Type: flop.FieldFloat},
				{Name: "comment_count", Type: flop.FieldInt},
				{Name: "repost_of", Type: flop.FieldRef, RefTable: "posts"},
			},
			CascadeDeletes: []string{"comments", "votes"},
		},
		{
			Name: "comments",
			Fields: []flop.Field{
				{Name: "body", Type: flop.FieldString, Required: true, Searchable: true, MaxLen: 10000},
				{Name: "author_id", Type: flop.FieldRef, RefTable: "users", Required: true},
				{Name: "post_id", Type: flop.FieldRef, RefTable: "posts", Required: true},
				{Name: "parent_id", Type: flop.FieldRef, RefTable: "comments", SelfRef: true},
				{Name: "depth", Type: flop.FieldInt},
				{Name: "path", Type: flop.FieldString},
				{Name: "score", Type: flop.FieldInt},
			},
			CascadeDeletes: []string{"comment_votes"},
		},
		{
			Name: "votes",
			Fields: []flop.Field{
				{Name: "user_id", Type: flop.FieldRef, RefTable: "users", Required: true},
				{Name: "post_id", Type: flop.FieldRef, RefTable: "posts", Required: true},
				{Name: "value", Type: flop.FieldInt},
			},
			UniqueConstraints: [][]string{{"user_id", "post_id"}},
		},
		{
			Name: "comment_votes",
			Fields: []flop.Field{
				{Name: "user_id", Type: flop.FieldRef, RefTable: "users", Required: true},
				{Name: "comment_id", Type: flop.FieldRef, RefTable: "comments", Required: true},
				{Name: "value", Type: flop.FieldInt},
			},
			UniqueConstraints: [][]string{{"user_id", "comment_id"}},
		},
	}

	for _, s := range schemas {
		if _, err := db.CreateTable(s); err != nil {
			log.Fatal(err)
		}
	}

	count, _ := db.Table("users").Count()
	if count > 0 {
		log.Println("Database already seeded. Use -force to reseed.")
		db.Close()
		return
	}

	rng := rand.New(rand.NewSource(42))
	start := time.Now()

	// ===== USERS =====
	// Create 10 "real" users with proper auth (for login), rest via direct insert
	log.Println("Creating 10,000 users...")
	t0 := time.Now()
	userIDs := make([]uint64, 0, 10000)

	// First 10 users with proper auth so they can log in with password "password123"
	for i := 0; i < 10; i++ {
		handle := fmt.Sprintf("user%d", i)
		user, err := authMgr.Register(
			fmt.Sprintf("user%d@example.com", i),
			"password123",
			map[string]any{
				"handle":       handle,
				"display_name": randomDisplayName(rng, i),
				"bio":          randomBio(rng),
				"karma":        rng.Intn(50000),
			},
		)
		if err != nil {
			log.Printf("user %d: %v", i, err)
			continue
		}
		userIDs = append(userIDs, user.ID)
	}
	log.Printf("  10 auth users created (%.1fs)", time.Since(t0).Seconds())

	// Remaining 9990 users via direct insert (skip argon2 hashing)
	// These users can't log in but exist for realistic data volume
	prehashedPw := "$argon2id$seed$seedhash" // placeholder, not a real hash
	for i := 10; i < 10000; i++ {
		handle := fmt.Sprintf("user%d", i)
		user, err := db.Insert("users", map[string]any{
			"email":        fmt.Sprintf("user%d@example.com", i),
			"password":     prehashedPw,
			"handle":       handle,
			"display_name": randomDisplayName(rng, i),
			"bio":          randomBio(rng),
			"karma":        rng.Intn(50000),
		})
		if err != nil {
			continue
		}
		userIDs = append(userIDs, user.ID)
		if (i+1)%2000 == 0 {
			log.Printf("  %d users created (%.1fs)", i+1, time.Since(t0).Seconds())
		}
	}
	log.Printf("  Done: %d users in %.1fs", len(userIDs), time.Since(t0).Seconds())

	// ===== COMMUNITIES =====
	log.Println("Creating 100 communities...")
	t0 = time.Now()
	communityIDs := make([]uint64, 0, 100)
	for i := 0; i < 100; i++ {
		creator := userIDs[rng.Intn(len(userIDs))]
		handle := communityHandles[i%len(communityHandles)]
		if i >= len(communityHandles) {
			handle = fmt.Sprintf("%s%d", handle, i)
		}
		community, err := db.Insert("communities", map[string]any{
			"name":         communityNames[i%len(communityNames)],
			"handle":       handle,
			"description":  communityDescs[rng.Intn(len(communityDescs))],
			"creator_id":   creator,
			"member_count": 1,
			"visibility":   "public",
		})
		if err != nil {
			log.Printf("community %d: %v", i, err)
			continue
		}
		communityIDs = append(communityIDs, community.ID)
		db.Insert("memberships", map[string]any{
			"user_id":      creator,
			"community_id": community.ID,
			"role":         "admin",
		})
	}
	log.Printf("  Done: %d communities in %.1fs", len(communityIDs), time.Since(t0).Seconds())

	// ===== MEMBERSHIPS =====
	log.Println("Creating memberships...")
	t0 = time.Now()
	memberCount := make(map[uint64]int)
	membershipCount := 0
	for _, cid := range communityIDs {
		numMembers := 20 + rng.Intn(200)
		shuffled := rng.Perm(len(userIDs))
		for j := 0; j < numMembers && j < len(shuffled); j++ {
			uid := userIDs[shuffled[j]]
			_, err := db.Insert("memberships", map[string]any{
				"user_id":      uid,
				"community_id": cid,
				"role":         "member",
			})
			if err == nil {
				memberCount[cid]++
				membershipCount++
			}
		}
	}
	for cid, extra := range memberCount {
		community, _ := db.Table("communities").Get(cid)
		if community != nil {
			current := int(flop.ToUint64(community.Data["member_count"]))
			db.Update("communities", cid, map[string]any{"member_count": current + extra})
		}
	}
	log.Printf("  Done: %d memberships in %.1fs", membershipCount, time.Since(t0).Seconds())

	// ===== POSTS =====
	// Pre-compute comment counts so we can set them at insert time (avoids expensive updates later)
	log.Println("Pre-computing post metadata...")
	type postPlan struct {
		score        int
		commentCount int
	}
	postPlans := make([]postPlan, 100000)
	for i := range postPlans {
		upvotes := rng.Intn(30)
		downvotes := rng.Intn(8)
		if rng.Float64() < 0.05 {
			upvotes = 50 + rng.Intn(200)
		}
		postPlans[i].score = upvotes - downvotes
		if rng.Float64() <= 0.30 {
			postPlans[i].commentCount = 1 + rng.Intn(3)
			if rng.Float64() < 0.03 {
				postPlans[i].commentCount = 4 + rng.Intn(5)
			}
		}
	}

	log.Println("Creating 100,000 posts...")
	t0 = time.Now()
	type postInfo struct {
		id           uint64
		score        int
		communityID  uint64
		commentCount int
	}
	posts := make([]postInfo, 0, 100000)
	now := time.Now()
	for i := 0; i < 100000; i++ {
		author := userIDs[rng.Intn(len(userIDs))]
		community := communityIDs[rng.Intn(len(communityIDs))]
		age := time.Duration(rng.Intn(30*24*60)) * time.Minute
		createdAt := now.Add(-age)
		plan := postPlans[i]

		data := map[string]any{
			"title":         randomTitle(rng),
			"body":          randomBody(rng),
			"author_id":     author,
			"community_id":  community,
			"score":         plan.score,
			"hot_rank":      hotRank(plan.score, createdAt),
			"comment_count": plan.commentCount,
		}
		if rng.Float64() < 0.10 {
			data["link"] = randomLink(rng)
			data["body"] = nil
		}

		post, err := db.Insert("posts", data)
		if err != nil {
			continue
		}
		posts = append(posts, postInfo{
			id:           post.ID,
			score:        plan.score,
			communityID:  community,
			commentCount: plan.commentCount,
		})
		if (i+1)%20000 == 0 {
			log.Printf("  %d posts created (%.1fs)", i+1, time.Since(t0).Seconds())
		}
	}
	log.Printf("  Done: %d posts in %.1fs", len(posts), time.Since(t0).Seconds())

	// ===== VOTES =====
	// Create votes that match the pre-computed scores
	log.Println("Creating votes...")
	t0 = time.Now()
	voteCount := 0
	for pi, p := range posts {
		// Generate enough votes to roughly match the score
		numUp := (p.score + abs(p.score)) / 2   // approximate
		numDown := (abs(p.score) - p.score) / 2
		if numUp < 0 { numUp = 0 }
		total := numUp + numDown
		if total == 0 {
			continue
		}
		if total > 50 {
			total = 50 // cap for speed
		}

		shuffled := rng.Perm(len(userIDs))
		for j := 0; j < total && j < len(shuffled); j++ {
			uid := userIDs[shuffled[j]]
			value := 1
			if j >= numUp {
				value = -1
			}
			_, err := db.Insert("votes", map[string]any{
				"user_id": uid,
				"post_id": p.id,
				"value":   value,
			})
			if err == nil {
				voteCount++
			}
		}
		if (pi+1)%20000 == 0 {
			log.Printf("  %d/%d posts voted (%.1fs)", pi+1, len(posts), time.Since(t0).Seconds())
		}
	}
	log.Printf("  Done: %d votes in %.1fs", voteCount, time.Since(t0).Seconds())

	// ===== COMMENTS =====
	// comment_count was pre-set at post creation time, so no update needed after.
	// Use a global counter for path to avoid needing an Update after Insert.
	log.Println("Creating comments...")
	t0 = time.Now()
	commentCount := 0
	commentSeq := uint64(0)
	type commentRef struct {
		id    uint64
		depth int
		path  string
	}

	for pi, p := range posts {
		if p.commentCount == 0 {
			continue
		}

		var refs []commentRef
		for j := 0; j < p.commentCount; j++ {
			author := userIDs[rng.Intn(len(userIDs))]
			depth := 0
			parentID := uint64(0)
			basePath := ""

			if len(refs) > 0 && rng.Float64() < 0.3 {
				parent := refs[rng.Intn(len(refs))]
				if parent.depth < 3 {
					depth = parent.depth + 1
					parentID = parent.id
					basePath = parent.path
				}
			}

			commentSeq++
			cPath := fmt.Sprintf("%s/%08d", basePath, commentSeq)

			c, err := db.Insert("comments", map[string]any{
				"body":      randomComment(rng),
				"author_id": author,
				"post_id":   p.id,
				"parent_id": parentID,
				"depth":     depth,
				"path":      cPath,
				"score":     rng.Intn(20) - 2,
			})
			if err != nil {
				continue
			}

			refs = append(refs, commentRef{id: c.ID, depth: depth, path: cPath})
			commentCount++
		}

		if (pi+1)%20000 == 0 {
			log.Printf("  %d/%d posts commented (%.1fs, %d comments)", pi+1, len(posts), time.Since(t0).Seconds(), commentCount)
		}
	}
	log.Printf("  Done: %d comments in %.1fs", commentCount, time.Since(t0).Seconds())

	// ===== FLUSH =====
	log.Println("Flushing to disk...")
	t0 = time.Now()
	if err := db.Flush(); err != nil {
		log.Printf("flush error: %v", err)
	}
	db.Close()
	log.Printf("  Done in %.1fs", time.Since(t0).Seconds())

	log.Printf("\nSeed complete in %.1fs", time.Since(start).Seconds())
	log.Printf("  Users:       %d", len(userIDs))
	log.Printf("  Communities: %d", len(communityIDs))
	log.Printf("  Posts:       %d", len(posts))
	log.Printf("  Votes:       %d", voteCount)
	log.Printf("  Comments:    %d", commentCount)
	log.Printf("  Memberships: %d", membershipCount)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func hotRank(score int, created time.Time) float64 {
	order := math.Log10(math.Max(math.Abs(float64(score)), 1))
	sign := 0.0
	if score > 0 {
		sign = 1
	} else if score < 0 {
		sign = -1
	}
	seconds := float64(created.Unix()-1134028003) / 45000.0
	return sign*order + seconds
}

func randomDisplayName(rng *rand.Rand, i int) string {
	return fmt.Sprintf("%s %s", firstNames[rng.Intn(len(firstNames))], lastNames[rng.Intn(len(lastNames))])
}

func randomBio(rng *rand.Rand) string {
	if rng.Float64() < 0.4 { return "" }
	return bios[rng.Intn(len(bios))]
}

func randomTitle(rng *rand.Rand) string { return titles[rng.Intn(len(titles))] }

func randomBody(rng *rand.Rand) string {
	if rng.Float64() < 0.3 { return "" }
	return bodies[rng.Intn(len(bodies))]
}

func randomComment(rng *rand.Rand) string { return comments[rng.Intn(len(comments))] }

func randomLink(rng *rand.Rand) string { return links[rng.Intn(len(links))] }

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil { return "", err }
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil { return dir, nil }
		next := filepath.Dir(dir)
		if next == dir { return "", os.ErrNotExist }
		dir = next
	}
}

// ===== Data pools =====

var firstNames = []string{
	"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
	"David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
	"Thomas", "Sarah", "Charles", "Karen", "Chris", "Lisa", "Daniel", "Nancy",
	"Alex", "Betty", "Sam", "Margaret", "Emma", "Oliver", "Noah", "Liam",
	"Sophia", "Ava", "Luna", "Mia", "Aria", "Elijah", "Lucas", "Mason",
}

var lastNames = []string{
	"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
	"Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
	"Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
	"White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
}

var bios = []string{
	"Just here for the memes.", "Software engineer by day, Redditor by night.",
	"Coffee enthusiast and cat person.", "Professional lurker, occasional poster.",
	"I like turtles.", "Probably should be working right now.",
	"Long time lurker, first time poster.", "Here to learn and share.",
	"Opinions are my own, not my employer's.", "Life is short, eat dessert first.",
	"Aspiring astronaut, settling for keyboard warrior.", "Dog lover, pizza connoisseur.",
	"Currently procrastinating on something important.", "It's not a bug, it's a feature.",
	"I came for the content, stayed for the comments.",
}

var communityHandles = []string{
	"programming", "gaming", "science", "technology", "news",
	"funny", "askreddit", "worldnews", "todayilearned", "movies",
	"music", "sports", "books", "food", "travel",
	"photography", "art", "diy", "fitness", "personalfinance",
	"history", "space", "nature", "philosophy", "psychology",
	"math", "physics", "biology", "chemistry", "engineering",
	"datascience", "machinelearning", "webdev", "gamedev", "linux",
	"android", "apple", "windows", "cybersecurity", "networking",
	"cars", "motorcycles", "cycling", "running", "hiking",
	"cooking", "baking", "gardening", "woodworking", "knitting",
	"dogs", "cats", "aquariums", "reptiles", "birdwatching",
	"chess", "boardgames", "videogames", "retrogaming", "pcgaming",
	"anime", "manga", "comics", "scifi", "fantasy",
	"horror", "mystery", "romance", "poetry", "writing",
	"startups", "entrepreneur", "investing", "crypto", "economics",
	"politics", "law", "education", "languages", "linguistics",
	"astronomy", "geology", "oceanography", "meteorology", "ecology",
	"architecture", "interiordesign", "fashion", "streetwear", "sneakers",
	"hiphop", "rock", "jazz", "classical", "electronic",
	"filmmaking", "screenwriting", "animation", "vfx", "photography2",
}

var communityNames = []string{
	"Programming", "Gaming", "Science", "Technology", "News",
	"Funny", "Ask Reddit", "World News", "Today I Learned", "Movies",
	"Music", "Sports", "Books", "Food", "Travel",
	"Photography", "Art", "DIY", "Fitness", "Personal Finance",
	"History", "Space", "Nature", "Philosophy", "Psychology",
	"Mathematics", "Physics", "Biology", "Chemistry", "Engineering",
	"Data Science", "Machine Learning", "Web Development", "Game Dev", "Linux",
	"Android", "Apple", "Windows", "Cybersecurity", "Networking",
	"Cars", "Motorcycles", "Cycling", "Running", "Hiking",
	"Cooking", "Baking", "Gardening", "Woodworking", "Knitting",
	"Dogs", "Cats", "Aquariums", "Reptiles", "Bird Watching",
	"Chess", "Board Games", "Video Games", "Retro Gaming", "PC Gaming",
	"Anime", "Manga", "Comics", "Sci-Fi", "Fantasy",
	"Horror", "Mystery", "Romance", "Poetry", "Writing",
	"Startups", "Entrepreneur", "Investing", "Crypto", "Economics",
	"Politics", "Law", "Education", "Languages", "Linguistics",
	"Astronomy", "Geology", "Oceanography", "Meteorology", "Ecology",
	"Architecture", "Interior Design", "Fashion", "Streetwear", "Sneakers",
	"Hip Hop", "Rock", "Jazz", "Classical", "Electronic Music",
	"Filmmaking", "Screenwriting", "Animation", "VFX", "Photography & Film",
}

var communityDescs = []string{
	"A place to discuss and share content about this topic.",
	"Welcome! Share your thoughts, questions, and discoveries.",
	"The best community for enthusiasts and professionals alike.",
	"Come for the content, stay for the discussions.",
	"A friendly community for beginners and experts.",
	"Share, learn, and grow together.",
	"News, discussions, and interesting finds.",
	"The go-to subreddit for everything related to this topic.",
	"Quality content and thoughtful discussions.",
	"Join us for daily discussions and weekly events.",
}

var titles = []string{
	"What's the best advice you've ever received?",
	"TIL that honey never spoils. Archaeologists found 3000 year old honey in Egyptian tombs.",
	"Just finished my first project - feedback welcome!",
	"This changed my entire perspective on programming",
	"Unpopular opinion: tabs are better than spaces",
	"How do you stay motivated when working on long projects?",
	"I built this over the weekend - what do you think?",
	"The most underrated feature nobody talks about",
	"Why I switched from X to Y and never looked back",
	"Beginner's guide to getting started in 2026",
	"What common misconception drives you crazy?",
	"This bug took me 3 days to find. The fix was one line.",
	"What are you working on this week?",
	"Hot take: complexity is the enemy of reliability",
	"Show HN: I built a Reddit clone with an embedded database",
	"The real reason big tech interviews are broken",
	"What's your unpopular technical opinion?",
	"I've been doing this for 10 years. Here's what I wish I knew.",
	"Finally got my setup just right. No more changes!",
	"Is anyone else frustrated with the current state of X?",
	"A simple trick that improved my productivity 10x",
	"Deep dive: How does this actually work under the hood?",
	"Discussion: What will the next decade look like?",
	"Weekly thread: Share what you're proud of",
	"PSA: You should probably update your dependencies",
	"Comparison: A vs B vs C - which one wins?",
	"I asked GPT to write my code. Here's what happened.",
	"Nostalgia thread: What was your first experience with this?",
	"Can we talk about the elephant in the room?",
	"ELI5: How does this complex thing actually work?",
	"What's the most elegant solution you've ever seen?",
	"I made a terrible mistake and here's what I learned",
	"Study shows surprising results about productivity",
	"The complete guide nobody asked for but everyone needs",
	"Why the old way was actually better",
	"Showdown: Traditional vs Modern approaches",
	"My journey from complete beginner to professional",
	"The one thing that changed everything for me",
	"Why I'm leaving and where I'm going",
	"You're probably doing this wrong (and that's okay)",
	"Breaking: Major announcement just dropped",
	"Rant: Why can't we have nice things?",
	"Appreciation post: Thank you to this community",
	"Controversial opinion: Fight me in the comments",
	"Tutorial: Build this cool thing in 30 minutes",
	"What nobody tells you about working in this field",
	"The dark side of this popular trend",
	"Let's settle this debate once and for all",
	"Fun fact: Something you probably didn't know",
	"What do you do differently from everyone else?",
}

var bodies = []string{
	"I've been thinking about this for a while and wanted to share my thoughts with the community. Would love to hear your perspectives on this.\n\nBasically, I think we need to reconsider how we approach this problem. The traditional methods work, but there might be better alternatives.",
	"So I ran into this issue yesterday and after hours of debugging, I finally found the root cause. Posting here in case anyone else runs into the same problem.\n\nThe fix was surprisingly simple once I understood what was happening.",
	"I've been working in this field for over 5 years now, and here are the key lessons I've learned along the way:\n\n1. Start simple and iterate\n2. Don't optimize prematurely\n3. Documentation is worth the investment\n4. Test your assumptions early\n5. Learn from failures",
	"Just wanted to share this resource I found. It's incredibly well-written and covers topics that are usually glossed over in most tutorials.\n\nHighly recommend for anyone at an intermediate level looking to level up.",
	"Am I the only one who thinks this? I feel like the community has been heading in this direction for a while, but nobody wants to address it directly.\n\nLet me know if you agree or if I'm completely off base here.",
	"This is a follow-up to my previous post. After implementing the suggestions from the comments, here are the results:\n\nPerformance improved by 3x and the code is much cleaner. Thanks everyone!",
	"I created a comprehensive comparison between the top options available right now. Here's what I found after testing each one for a month.\n\nTL;DR: There's no clear winner. It depends on your specific use case.",
	"Wanted to start a discussion about best practices. What does your workflow look like? I'm always looking to improve mine.\n\nCurrently I do X, then Y, then Z. Is there a better order?",
	"This might be a controversial take, but I think we're overcomplicating things. Sometimes the simple solution is the best solution.\n\nNot everything needs to be scalable to a million users on day one.",
	"Quick tip that might save someone hours of frustration: always check your assumptions before diving into debugging. Nine times out of ten, the bug is in your assumptions, not your code.",
	"After months of research, I've compiled everything I know into this post. Feel free to bookmark it for future reference.\n\nI'll update it as I learn more.",
	"I interviewed at 15 companies last month. Here's a breakdown of the process, questions asked, and what I learned about the current job market.",
	"Just shipped v2.0 of my project. The biggest changes are under the hood - complete rewrite of the core engine for better performance.\n\nBenchmarks show a 5x improvement in throughput.",
	"I've been lurking in this community for years and finally decided to contribute. Here's something I think everyone should know about.",
	"Hot take: We spend too much time debating tools and not enough time actually building things. The best tool is the one you know well.",
}

var comments = []string{
	"This is great, thanks for sharing!",
	"I disagree. Here's why...",
	"Can confirm, had the same experience.",
	"Underrated comment right here.",
	"This should be higher up.",
	"Thanks, this saved me hours!",
	"I've been saying this for years.",
	"Can you elaborate on this point?",
	"Source?",
	"As someone who works in this field, I can confirm this is accurate.",
	"This is the way.",
	"Came here to say exactly this.",
	"Interesting perspective, I hadn't thought of it that way.",
	"Instructions unclear, stuck in infinite loop.",
	"This is why I love this community.",
	"Based on my experience, I'd add that you should also consider...",
	"I tried this approach and it worked perfectly. Thanks!",
	"Respectfully disagree. The evidence suggests otherwise.",
	"Can you share more details about your setup?",
	"I wish I had known this sooner.",
	"Saving this for later. Incredible resource.",
	"To add to this, another important factor is...",
	"The real LPT is always in the comments.",
	"Serious question: how does this compare to the alternatives?",
	"Great analysis! One thing I'd add though...",
	"This is the kind of content I come here for.",
	"You're not wrong, but it's more nuanced than that.",
	"Bold claim. Let's see the benchmarks.",
	"This thread did not disappoint.",
	"I've been on the fence about this, but your post convinced me.",
}

var links = []string{
	"https://example.com/interesting-article",
	"https://example.com/breaking-news",
	"https://blog.example.com/deep-dive-into-topic",
	"https://github.com/example/cool-project",
	"https://youtube.com/watch?v=example",
	"https://arxiv.org/abs/2024.12345",
	"https://medium.com/example-publication/great-post",
	"https://dev.to/example/tutorial-post",
	"https://news.example.com/tech/2026/story",
	"https://example.org/research/findings",
}

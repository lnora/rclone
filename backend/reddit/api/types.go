package api

// ListItems of Pushshift items
type ListItems struct {
	Data []Item `json:"data"`
}

// Item Pushshift
type Item struct {
	Author     string `json:"author"`
	Subreddit  string `json:"subreddit"`
	CreatedUtc int64  `json:"created_utc"`
	ID         string `json:"id"`
	Domain     string `json:"domain"`
	PostHint   string `json:"post_hint"`
	Media      struct {
		Oembed struct {
			ThumbnailURL string `json:"thumbnail_url"`
		} `json:"oembed"`
	} `json:"media"`
	URL     string `json:"url"`
	Preview struct {
		Images []struct {
			ID          string `json:"id"`
			Resolutions []struct {
				URL string `json:"url"`
			} `json:"resolutions"`
			Source struct {
				URL string `json:"url"`
			} `json:"source"`
		} `json:"images"`
		RedditVideoPreview struct {
			FallbackURL string `json:"fallback_url"`
		} `json:"reddit_video_preview"`
	} `json:"preview"`
	Stat bool
}

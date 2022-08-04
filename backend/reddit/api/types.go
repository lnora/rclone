package api

type ListItems struct {
	Data []Item `json:"data"`
}

type Item struct {
	Author     string `json:"author"`
	Subreddit  string `json:"subreddit"`
	CreatedUtc int64  `json:"created_utc"`
	Id         string `json:"id"`
	Domain     string `json:"domain"`
	PostHint   string `json:"post_hint"`
	Media      struct {
		Oembed struct {
			ThumbnailUrl string `json:"thumbnail_url"`
		} `json:"oembed"`
	} `json:"media"`
	Url     string `json:"url"`
	Preview struct {
		Images []struct {
			ID          string `json:"id"`
			Resolutions []struct {
				Url string `json:"url"`
			} `json:"resolutions"`
		} `json:"images"`
		RedditVideoPreview struct {
			FallbackUrl string `json:"fallback_url"`
		} `json:"reddit_video_preview"`
	} `json:"preview"`
	Stat bool
}

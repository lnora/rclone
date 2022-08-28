package api

// ListItems of Pushshift items
type ListItems struct {
	Data []Item `json:"data"`
}

// Item Pushshift
type Item struct {
	Author      string `json:"author"`
	Subreddit   string `json:"subreddit"`
	CreatedUtc  int64  `json:"created_utc"`
	ID          string `json:"id"`
	Domain      string `json:"domain"`
	PostHint    string `json:"post_hint"`
	GalleryData struct {
		Items []struct {
			ID      int    `json:"id"`
			MediaID string `json:"media_id"`
		} `json:"items"`
	} `json:"gallery_data"`
	IsGallery bool `json:"is_gallery"`
	Media     struct {
		Oembed struct {
			ThumbnailURL string `json:"thumbnail_url"`
		} `json:"oembed"`
	} `json:"media"`
	MediaMetadata map[string]struct {
		E  string `json:"e"`
		ID string `json:"id"`
		M  string `json:"m"`
		O  []struct {
			U string `json:"u"`
			X int    `json:"x"`
			Y int    `json:"y"`
		} `json:"o"`
		P []struct {
			U string `json:"u"`
			X int    `json:"x"`
			Y int    `json:"y"`
		} `json:"p"`
		S struct {
			U string `json:"u"`
			X int    `json:"x"`
			Y int    `json:"y"`
		} `json:"s"`
		Status string `json:"status"`
	} `json:"media_metadata"`
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

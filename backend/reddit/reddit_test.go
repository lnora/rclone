// Test Reddit filesystem interface
package reddit_test

import (
	"testing"

	"github.com/rclone/rclone/backend/reddit"
	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestReddit:",
		NilObject:  (*reddit.Object)(nil),
	})
}

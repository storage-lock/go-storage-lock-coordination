package coordination

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var ownerSequence atomic.Uint64

// newSessionOwner creates an owner value that is unique per acquisition.
// The random component prevents two independently created clients that use the
// same human-readable prefix from accidentally being treated as a reentrant owner.
func newSessionOwner(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err == nil {
		return fmt.Sprintf("%s/%s", prefix, hex.EncodeToString(b[:]))
	}
	seq := ownerSequence.Add(1)
	return fmt.Sprintf("%s/%d-%d", prefix, time.Now().UnixNano(), seq)
}

package nats

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFSFallback_SaveMessage(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name    string
		subject string
		expect  string
		msg     []byte
	}{
		{
			name:    "success",
			subject: "subject",
			msg:     []byte(`{"key":"value"}`),
			expect:  `{"data":"eyJrZXkiOiJ2YWx1ZSJ9", "subject":"subject"}`,
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			t.Cleanup(func() {
				err := os.RemoveAll(dir)
				require.NoError(t, err)
			})

			fsFallback := NewFSFallback(dir)

			err := fsFallback.SaveMessage(t.Context(), tc.subject, tc.msg)
			require.NoError(t, err)

			dd, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, dd, 1)

			data, err := os.ReadFile(filepath.Join(dir, dd[0].Name()))
			require.NoError(t, err)
			require.JSONEq(t, tc.expect, string(data))
		})
	}
}

package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const slackTimeout = 10 * time.Second

const (
	colorCritical = "#8B1E1E"
	colorWarning  = "#C7771A"
	colorInfo     = "#6C8392"
)

type slackAttachment struct {
	Color string `json:"color"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

type slackPayload struct {
	Attachments []slackAttachment `json:"attachments"`
}

func slackColor(level Level) string {
	switch level {
	case LevelCritical:
		return colorCritical
	case LevelWarning:
		return colorWarning
	case LevelInfo:
		return colorInfo
	default:
		return colorInfo
	}
}

// postToSlack delivers one alert to a Slack incoming webhook.
func postToSlack(ctx context.Context, client *http.Client, webhookURL string, def Def, text string) error {
	body, err := json.Marshal(slackPayload{Attachments: []slackAttachment{{
		Color: slackColor(def.Level),
		Title: def.Title,
		Text:  text,
	}}})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack webhook returned status %d", resp.StatusCode)
	}

	return nil
}

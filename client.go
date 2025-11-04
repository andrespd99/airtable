// Copyright © 2020 Mike Berezin
//
// Use of this source code is governed by an MIT license.
// Details in the LICENSE file.

package airtable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/time/rate"
)

const (
	airtableBaseURL                 = "https://api.airtable.com/v0"
	airtableUploadAttachmentBaseURL = "https://content.airtable.com/v0"
	rateLimit                       = 4
)

var (
	// backoff settings for retrying 503 responses
	backoffMaxRetries = 3
	backoffBaseDelay  = 100 * time.Millisecond
	backoffMaxJitter  = 100 * time.Millisecond
)

// Client client for airtable api.
type Client struct {
	client                  *http.Client
	rateLimiter             *rate.Limiter
	baseURL                 string
	uploadAttachmentBaseURL string
	apiKey                  string
	// backoff parameters (per-client)
	maxRetries    int
	backoffBase   time.Duration
	backoffJitter time.Duration
}

// NewClient airtable client constructor
// your API KEY you can get on your account page
// https://airtable.com/account
func NewClient(apiKey string) *Client {
	return &Client{
		client:                  http.DefaultClient,
		rateLimiter:             rate.NewLimiter(rate.Limit(rateLimit), 1),
		apiKey:                  apiKey,
		baseURL:                 airtableBaseURL,
		uploadAttachmentBaseURL: airtableUploadAttachmentBaseURL,
		maxRetries:              backoffMaxRetries,
		backoffBase:             backoffBaseDelay,
		backoffJitter:           backoffMaxJitter,
	}
}

// SetBackoffRetries sets how many retries will be attempted on 503 responses.
func (at *Client) SetBackoffRetries(n int) {
	at.maxRetries = n
}

// SetBackoffBaseDelay sets the base delay used for exponential backoff.
func (at *Client) SetBackoffBaseDelay(d time.Duration) {
	at.backoffBase = d
}

// SetBackoffMaxJitter sets the maximum jitter added to backoff sleeps.
func (at *Client) SetBackoffMaxJitter(d time.Duration) {
	at.backoffJitter = d
}

// Set custom http client for custom usage
func (at *Client) SetCustomClient(client *http.Client) {
	at.client = client
}

// SetRateLimit rate limit setter for custom usage
// Airtable limit is 5 requests per second (we use 4)
// https://airtable.com/{yourDatabaseID}/api/docs#curl/ratelimits
func (at *Client) SetRateLimit(customRateLimit int) {
	at.rateLimiter = rate.NewLimiter(rate.Limit(customRateLimit), 1)
}

func (at *Client) SetBaseURL(baseURL string) error {
	url, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("failed to parse baseURL: %s", err)
	}

	if url.Scheme == "" {
		return fmt.Errorf("scheme of http or https must be specified")
	}

	if url.Scheme != "https" && url.Scheme != "http" {
		return fmt.Errorf("http or https baseURL must be used")
	}

	at.baseURL = url.String()

	return nil
}

func (at *Client) rateLimit(ctx context.Context) error {
	return at.rateLimiter.Wait(ctx)
}

func (at *Client) get(ctx context.Context, db, table, recordID string, params url.Values, target any) error {
	err := at.rateLimit(ctx)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/%s", at.baseURL, db, table)
	if recordID != "" {
		url += fmt.Sprintf("/%s", recordID)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", at.apiKey))

	req.URL.RawQuery = params.Encode()

	err = at.do(req, target)
	if err != nil {
		return err
	}

	return nil
}

func (at *Client) post(ctx context.Context, db, table string, data, response any) error {
	err := at.rateLimit(ctx)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/%s", at.baseURL, db, table)

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("cannot marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", at.apiKey))

	return at.do(req, response)
}

func (at *Client) postAttachment(ctx context.Context, db, recordID string, attachmentFieldIdOrName string, data Attachment, response any) error {
	err := at.rateLimit(ctx)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/%s/%s/uploadAttachment", at.uploadAttachmentBaseURL, db, recordID, attachmentFieldIdOrName)

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("cannot marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", at.apiKey))

	return at.do(req, response)
}

func (at *Client) delete(ctx context.Context, db, table string, recordIDs []string, target any) error {
	err := at.rateLimit(ctx)
	if err != nil {
		return err
	}

	rawURL := fmt.Sprintf("%s/%s/%s", at.baseURL, db, table)
	params := url.Values{}

	for _, recordID := range recordIDs {
		params.Add("records[]", recordID)
	}

	req, err := http.NewRequestWithContext(ctx, "DELETE", rawURL, nil)
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", at.apiKey))

	req.URL.RawQuery = params.Encode()

	err = at.do(req, target)
	if err != nil {
		return err
	}

	return nil
}

func (at *Client) patch(ctx context.Context, db, table, data, response any) error {
	err := at.rateLimit(ctx)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/%s", at.baseURL, db, table)

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("cannot marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", at.apiKey))

	return at.do(req, response)
}

func (at *Client) put(ctx context.Context, db, table, data, response any) error {
	err := at.rateLimit(ctx)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/%s", at.baseURL, db, table)

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("cannot marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", at.apiKey))

	return at.do(req, response)
}

func (at *Client) do(req *http.Request, response any) error {
	if req == nil {
		return errors.New("nil request")
	}

	url := req.URL.RequestURI()

	// Ensure request body can be replayed for retries. If GetBody is not
	// provided, read the body and set GetBody so we can recreate Body for
	// each attempt.
	if req.Body != nil && req.GetBody == nil {
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("reading request body before retrying: %w", err)
		}
		// reset original Body
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyBytes)), nil
		}
	}

	// Attempt requests with retries on 503. On non-503 non-2xx responses we
	// return immediately. On 503 we retry with exponential backoff + jitter.
	for attempt := 0; attempt <= backoffMaxRetries; attempt++ {
		creq := req.Clone(req.Context())
		if req.GetBody != nil {
			rc, err := req.GetBody()
			if err != nil {
				return fmt.Errorf("failed to get request body for retry: %w", err)
			}
			creq.Body = rc
		}

		resp, err := at.client.Do(creq)
		if err != nil {
			return fmt.Errorf("HTTP request failure on %s: %w", url, err)
		}

		// If it's a 503 and we still have retries left, drain and close the
		// body then sleep and retry. If this is the final attempt, let
		// makeHTTPClientError read the body (don't close it here).
		if resp.StatusCode == http.StatusServiceUnavailable {
			if attempt == backoffMaxRetries {
				// final attempt, return error; do not close body so the
				// error helper can read it.
				return makeHTTPClientError(url, resp)
			}

			// drain and close so idle connection can be reused
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			// exponential backoff with jitter
			sleep := backoffBaseDelay * time.Duration(1<<attempt)
			jitter := time.Duration(rand.Int63n(int64(backoffMaxJitter)))
			time.Sleep(sleep + jitter)

			// try again
			continue
		}

		// Non-2xx responses (other than 503 handled above)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return makeHTTPClientError(url, resp)
		}

		// Success path: read the body, close, and unmarshal
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("HTTP Read error on response for %s: %w", url, err)
		}

		err = json.Unmarshal(b, response)
		if err != nil {
			return fmt.Errorf("JSON decode failed on %s:\n%s\nerror: %w", url, string(b), err)
		}

		return nil
	}

	// Should never reach here
	return errors.New("request retry loop exhausted")
}

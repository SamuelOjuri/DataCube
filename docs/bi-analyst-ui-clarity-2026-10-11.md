# Analyst UI clarity review — 11 October 2026

The supplied screenshots show repeated unsuccessful attempts before a completed
answer to “Show stored Order Value by category.” The frontend source explains
the presentation problems:

- `Workspace.tsx` rendered every saved run as a separate full question and
  response, including repeated failed retries. It also displayed a large generic
  heading, introductory copy and slogans on saved conversations.
- `ResultCard.tsx` displayed the aggregate, generated answer text, chart and
  table together. The backend's `workflow/evidence.py` generates the answer text
  from the same aggregate and up to eight contributors, duplicating the values
  rather than adding an explanation.
- Full scope metadata and a coverage/freshness diagnostics block preceded the
  chart. Source-row and known-value counts appeared in the default table.
- The fixed-width vertical category chart had angled labels and used only part
  of the available desktop width.

The screenshot's 15,220 matching source records include 11,295 known values:
3,925 records have no stored value. The £49,559,451.71 figure therefore needs a
clear known-subtotal qualification. Source freshness is unknown. Those facts
remain visible; the other diagnostics can be read on demand.

## Local changes

Consecutive unsuccessful attempts of the same trimmed question now belong to
one displayed question. The most recent attempt remains visible and actionable;
earlier attempts remain available in an expandable history. Different questions
and completed answers keep their separate positions. No saved runs are deleted
or changed, and retry and follow-up submission behavior is unchanged.

The default result view shows the metric, scope, aggregate, relevant missing-value
or limited-result warning and chart. Exact values can be expanded, with source
counts available through a checkbox. Results that cannot be charted still show
their table immediately. Original answer text, definitions, coverage counters,
source versions and export scope are available through disclosures. Comparison
scope, evaluation-data labels and scope-change notices are retained.

Category bars are horizontal and ordered by value for single-series breakdowns.
Month axes retain chronological order. The chart resizes with its container;
labels remain readable without horizontal page overflow on mobile. Smaller
saved-conversation headings, less spacing and conversation timestamps reduce
clutter and distinguish history entries with the same title.

## Verification

- 19 frontend unit tests passed.
- 15 browser tests passed, including the production build and desktop/mobile
  accessibility checks, with no accessibility violations.
- Browser regressions cover three failed retries followed by a completed answer,
  a latest failure that remains retryable, preservation of unrelated questions
  and completed answers, optional values/source counts, retained diagnostics,
  fourteen categories with long labels, exact values across table pages and
  chart resizing between desktop and mobile.
- Existing sign-in, session expiry, SSE replay, clarification idempotency,
  follow-ups, cancellation, CSV, project detail and feedback checks passed.
- Desktop and mobile screenshots were visually reviewed. Screenshots use local
  synthetic fixtures, not production results. The build retains its existing
  large chart-library chunk warning.

Preview artifacts: `outputs/analyst-simplified-desktop.png`,
`outputs/analyst-category-desktop.png`, `outputs/analyst-category-mobile.png`.

The in-app browser was unavailable in this session, so the deployed signed-in
conversation was not inspected. This review establishes the rendering causes,
not the causes of the earlier execution failures. These changes are local and
require a frontend build/deployment to appear on Netlify. No backend deployment
or database migration was performed or is required for this UI change.

## Homepage refinement

The signed-in homepage retains the white, light-grey and burgundy palette and
locally hosted Open Sans font. Its supporting copy now names the business topics
instead of repeating a generic instruction. The heading is better balanced with
16px supporting text; sidebar titles use 14px text with 12px timestamps.

Suggestion cards now have short, semibold topic titles, readable questions and
right-arrow icons rather than external-link arrows. Desktop cards are more
compact, and the gap before the question composer is 24px. The order-value
suggestion keeps its stored-value qualification. Selecting any card still only
fills the editable question; it never submits automatically.

The composer shares the cards' 12px corner radius, removes the internal divider
and uses its outer border and ring to indicate input focus. The "Ask analyst"
button has a compact 44px height, a circular send-icon treatment, a restrained
shadow and distinct hover and keyboard-focus states. Its burgundy background
and white text are retained when disabled, with no shadow or hover response.
Empty or whitespace-only questions, active runs and unconfirmed submissions
retain their existing submission guards. Follow-up context and all conversation
actions are unchanged. New transitions respect reduced-motion preferences.

### Homepage verification

- 19 frontend unit tests passed with `npm test`.
- All 15 existing browser regressions passed. The two new homepage tests passed
  in the final focused run with `npm run test:browser -- --grep "home page"`.
- The browser runner's production build and TypeScript check passed; the existing
  large chart-library chunk warning remains.
- The new checks cover all three exact suggested questions, no automatic
  submission, mouse and keyboard submission, visible keyboard focus, burgundy
  and white disabled styling, readable text sizes, compact desktop cards, the
  24px gap and the exact 44px button height.
- Homepage accessibility checks found no violations at desktop, tablet and
  mobile sizes. Widths of 768px, 390px and 320px have no horizontal page overflow.
- Desktop and mobile screenshots were visually reviewed using synthetic local
  fixtures, not production data:
  [desktop preview](../outputs/analyst-home-desktop.png) and
  [mobile preview](../outputs/analyst-home-mobile.png).

These refinements are local and require a frontend build with the deployed API
origin and a Netlify deployment. No backend change or migration is required.

## Submission acknowledgement states

The pending request previously triggered "Submission was not confirmed" as soon
as a question or clarification was sent, even while its response was still in
flight. Pending requests now explicitly distinguish `submitting` from
`unconfirmed` delivery.

While awaiting a response, the UI announces "Submitting your question..." or
"Submitting your clarification..." through a polite status message and does not
offer a retry. A successful acknowledgement clears that status and hands off to
the existing saved-run progress UI. Only an uncertain request failure shows the
unconfirmed-delivery warning. Retrying returns to normal submitting status and
uses the original request body and idempotency key.

Known rejections (400, 404, 409 and 422) still clear the pending request, surface
the existing error and allow the question to be revised. Other request failures
remain visible and retain the safe-retry request. Submission and navigation
guards, suggestion selection, follow-up context and run cancellation are
unchanged. This frontend correction does not resolve an actual lost response or
infer acceptance from the presence of a background run.

### Submission-state verification

- 19 frontend unit tests passed.
- 13 targeted browser tests passed in two runs. Four new tests cover delayed
  acknowledgements for suggested and typed questions, a lost acknowledgement
  followed by a safe retry, and all four definite-rejection status codes.
- The existing clarification retry test now also checks delayed acknowledgement
  and the transition from normal progress to uncertain delivery.
- Synthetic response gates verify the in-flight UI while a backend fixture has
  already registered a running task. Retrying after a lost acknowledgement sends
  the identical request and leaves exactly one saved run.
- Existing homepage, keyboard, accessibility, streaming replay, follow-up,
  cancellation, interrupted-run retry, saved history and session tests passed.
- The production build and TypeScript check passed. The existing large
  chart-library chunk warning remains.

The fix is local and requires frontend deployment; no backend change or database
migration is required.
